# frozen_string_literal: true

module Manifold
  module Terraform
    # Handles building metrics SQL for manifold routines
    class MetricsBuilder
      def initialize(manifold_config)
        @manifold_config = manifold_config
      end

      def build_metrics_struct(group_config = nil)
        return "" unless group_config || @manifold_config["metrics"]

        if group_config
          build_group_struct("metrics", group_config)
        else
          metric_groups = @manifold_config["metrics"].map do |group_name, config|
            build_group_struct(group_name, config)
          end
          metric_groups.join(",\n")
        end
      end

      private

      def build_group_struct(group_name, group_config)
        return "" unless valid_group_config?(group_config)

        breakout_structs = build_breakout_structs(group_config)
        return "" if breakout_structs.empty?

        "\tSTRUCT(\n#{breakout_structs.join(",\n")}\n\t) AS #{group_name}"
      end

      def valid_group_config?(group_config)
        group_config["breakouts"] &&
          group_config["aggregations"] &&
          !group_config["breakouts"].empty? &&
          !group_config["aggregations"].empty?
      end

      def build_breakout_structs(group_config)
        group_config["breakouts"].map do |name, config|
          build_breakout_struct(name, config, group_config)
        end.compact
      end

      def build_breakout_struct(name, config, group_config)
        condition = build_breakout_condition(name, config)
        metrics = build_breakout_metrics(group_config, condition)
        return if metrics.empty?

        "\t\tSTRUCT(\n\t\t\t#{metrics}\n\t\t) AS #{name}"
      end

      def build_breakout_metrics(group_config, condition)
        metrics = []
        add_count_metrics(metrics, group_config, condition)
        add_sum_metrics(metrics, group_config, condition)
        metrics.join(",\n\t\t\t")
      end

      def add_count_metrics(metrics, group_config, condition)
        return unless group_config.dig("aggregations", "countif")

        metrics << "COUNTIF(#{condition}) AS #{group_config["aggregations"]["countif"]}"
      end

      def add_sum_metrics(metrics, group_config, condition)
        group_config.dig("aggregations", "sumif")&.each do |name, config|
          metrics << "SUM(IF(#{condition}, #{config["field"]}, 0)) AS #{name}"
        end
      end

      def build_breakout_condition(_name, config)
        config
      end
    end

    # Handles building metrics SQL for manifold routines
    class MetricsSQLBuilder
      def initialize(name, manifold_config)
        @name = name
        @manifold_config = manifold_config
      end

      def build_metric_ctes(metrics_builder, &)
        @manifold_config["metrics"].map do |group_name, group_config|
          build_metric_cte(group_name, group_config, metrics_builder, &)
        end.join("\n")
      end

      def build_metrics_select
        <<~SQL
          SELECT
              id,
              timestamp,
              #{build_metrics_struct}
            FROM #{build_metric_joins}
        SQL
      end

      private

      def build_metric_cte(group_name, group_config, metrics_builder, &)
        <<~SQL
          WITH #{group_name.capitalize}Metrics AS (
            SELECT
              id,
              TIMESTAMP_TRUNC(#{timestamp_field}, #{interval}) timestamp,
              STRUCT(
                #{block_given? ? yield : metrics_builder.build_metrics_struct(group_config)}
              ) metrics
            FROM #{group_config["source"]}
            #{build_filter(group_config)}
            GROUP BY 1, 2
          )
        SQL
      end

      def build_metrics_struct
        metric_groups = @manifold_config["metrics"].keys
        metric_groups.map { |group| "#{group.capitalize}Metrics.metrics #{group}" }.join(",\n    ")
      end

      def build_metric_joins
        metric_groups = @manifold_config["metrics"].keys
        joins = metric_groups.map { |group| "#{group.capitalize}Metrics" }
        first = joins.shift
        return first if joins.empty?

        "#{first}\n  #{joins.map { |table| "FULL OUTER JOIN #{table} USING (id, timestamp)" }.join("\n  ")}"
      end

      def build_filter(group_config)
        return "" unless group_config["filter"]

        "WHERE #{group_config["filter"]}"
      end

      def interval
        @manifold_config&.dig("timestamp", "interval") || "DAY"
      end

      def timestamp_field
        @manifold_config&.dig("timestamp", "field")
      end
    end

    # Handles building SQL for manifold routines
    class SQLBuilder
      def initialize(name, manifold_config)
        @name = name
        @manifold_config = manifold_config
        @metrics_builder = MetricsSQLBuilder.new(name, manifold_config)
      end

      def build_manifold_merge_sql
        return "" unless valid_config?

        <<~SQL
          MERGE #{@name}.Manifold AS target USING (
            #{build_source_query}
          ) AS source
          #{build_merge_conditions}
          #{build_merge_actions}
        SQL
      end

      def build_dimensions_merge_sql(source_sql)
        <<~SQL
          MERGE #{@name}.Dimensions AS TARGET
          USING (
            #{source_sql}
          ) AS source
          ON source.id = target.id
          WHEN MATCHED THEN UPDATE SET target.dimensions = source.dimensions
          WHEN NOT MATCHED THEN INSERT ROW;
        SQL
      end

      private

      def valid_config?
        source_table && timestamp_field && @manifold_config["metrics"]
      end

      def source_table
        first_group = @manifold_config["metrics"]&.values&.first
        first_group&.dig("source")
      end

      def timestamp_field
        @manifold_config&.dig("timestamp", "field")
      end

      def build_source_query
        <<~SQL
          WITH Metrics AS (
            #{@metrics_builder.build_metrics_select}
          )

          #{build_final_select}
        SQL
      end

      def build_final_select
        <<~SQL
          SELECT
            id,
            timestamp,
            Dimensions.dimensions,
            (SELECT AS STRUCT Metrics.* EXCEPT(id, timestamp)) metrics
          FROM Metrics
          JOIN #{@name}.Dimensions USING (id)
        SQL
      end

      def build_merge_conditions
        "ON source.id = target.id AND source.timestamp = target.timestamp"
      end

      def build_merge_actions
        <<~SQL
          WHEN MATCHED THEN
            UPDATE SET
              metrics = source.metrics,
              dimensions = source.dimensions
          WHEN NOT MATCHED THEN
            INSERT ROW;
        SQL
      end
    end

    # Handles building table configurations
    class TableConfigBuilder
      def initialize(name)
        @name = name
      end

      def build_table_configs
        {
          "dimensions" => dimensions_table_config,
          "manifold" => manifold_table_config
        }
      end

      private

      def dimensions_table_config
        build_table_config("Dimensions")
      end

      def manifold_table_config
        build_table_config("Manifold")
      end

      def build_table_config(table_id)
        {
          "dataset_id" => @name,
          "project" => "${var.project_id}",
          "table_id" => table_id,
          "schema" => "${file(\"${path.module}/tables/#{table_id.downcase}.json\")}",
          "depends_on" => ["google_bigquery_dataset.#{@name}"]
        }
      end
    end

    # Represents a Terraform configuration for a Manifold workspace.
    class WorkspaceConfiguration < Configuration
      attr_reader :name
      attr_writer :dimensions_config, :manifold_config

      def initialize(name)
        super()
        @name = name
        @vectors = []
        @dimensions_config = nil
      end

      def add_vector(vector_config)
        @vectors << vector_config
      end

      def as_json
        {
          "variable" => variables_block,
          "resource" => {
            "google_bigquery_dataset" => dataset_config,
            "google_bigquery_table" => TableConfigBuilder.new(name).build_table_configs,
            "google_bigquery_routine" => routine_config
          }.compact
        }
      end

      private

      def variables_block
        {
          "project_id" => {
            "description" => "The GCP project ID where resources will be created",
            "type" => "string"
          }
        }
      end

      def dataset_config
        {
          name => {
            "dataset_id" => name,
            "project" => "${var.project_id}",
            "location" => "US"
          }
        }
      end

      def routine_config
        routines = {
          "merge_dimensions" => dimensions_routine_attributes,
          "merge_manifold" => manifold_routine_attributes
        }.compact

        routines.empty? ? nil : routines
      end

      def dimensions_routine_attributes
        return nil if @vectors.empty? || @dimensions_config.nil?

        {
          "dataset_id" => name,
          "project" => "${var.project_id}",
          "routine_id" => "merge_dimensions",
          "routine_type" => "PROCEDURE",
          "language" => "SQL",
          "definition_body" => "${file(\"${path.module}/routines/merge_dimensions.sql\")}",
          "depends_on" => ["google_bigquery_dataset.#{name}"]
        }
      end

      def manifold_routine_attributes
        {
          "dataset_id" => name,
          "project" => "${var.project_id}",
          "routine_id" => "merge_manifold",
          "routine_type" => "PROCEDURE",
          "language" => "SQL",
          "definition_body" => "${file(\"${path.module}/routines/merge_manifold.sql\")}",
          "depends_on" => ["google_bigquery_dataset.#{name}"]
        }
      end
    end
  end
end
