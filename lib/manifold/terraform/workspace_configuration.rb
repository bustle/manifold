# frozen_string_literal: true

module Manifold
  module Terraform
    # Handles building metrics SQL for manifold routines
    class MetricsBuilder
      def initialize(manifold_config)
        @manifold_config = manifold_config
      end

      def build_metrics_struct
        return "" unless @manifold_config&.dig("contexts") && @manifold_config&.dig("metrics")

        context_structs = @manifold_config["contexts"].map do |name, config|
          condition = build_context_condition(name, config)
          metrics = build_context_metrics(condition)
          "STRUCT(#{metrics}) AS #{name}"
        end

        context_structs.join(",\n")
      end

      private

      def build_context_metrics(condition)
        metrics = []
        add_count_metrics(metrics, condition)
        add_sum_metrics(metrics, condition)
        metrics.join(",\n")
      end

      def add_count_metrics(metrics, condition)
        return unless @manifold_config.dig("metrics", "countif")

        metrics << "COUNTIF(#{condition}) AS #{@manifold_config["metrics"]["countif"]}"
      end

      def add_sum_metrics(metrics, condition)
        @manifold_config.dig("metrics", "sumif")&.each do |name, config|
          metrics << "SUM(IF(#{condition}, #{config["field"]}, 0)) AS #{name}"
        end
      end

      def build_context_condition(_name, config)
        return config unless config.is_a?(Hash)

        operator = config["operator"]
        fields = config["fields"]
        build_operator_condition(operator, fields)
      end

      def build_operator_condition(operator, fields)
        conditions = fields.map { |f| @manifold_config["contexts"][f] }
        case operator
        when "AND", "OR" then join_conditions(conditions, operator)
        when "NOT" then negate_condition(conditions.first)
        when "NAND", "NOR" then negate_joined_conditions(conditions, operator[1..])
        when "XOR" then build_xor_condition(conditions)
        when "XNOR" then build_xnor_condition(conditions)
        else config
        end
      end

      def join_conditions(conditions, operator)
        conditions.join(" #{operator} ")
      end

      def negate_condition(condition)
        "NOT (#{condition})"
      end

      def negate_joined_conditions(conditions, operator)
        "NOT (#{join_conditions(conditions, operator)})"
      end

      def build_xor_condition(conditions)
        "(#{conditions[0]} AND NOT #{conditions[1]}) OR (NOT #{conditions[0]} AND #{conditions[1]})"
      end

      def build_xnor_condition(conditions)
        "(#{conditions[0]} AND #{conditions[1]}) OR (NOT #{conditions[0]} AND NOT #{conditions[1]})"
      end
    end

    # Handles building SQL for manifold routines
    class SQLBuilder
      def initialize(name, manifold_config)
        @name = name
        @manifold_config = manifold_config
      end

      def build_manifold_merge_sql(_metrics_builder, &)
        return "" unless valid_config?

        <<~SQL
          MERGE #{@name}.Manifold AS target USING (
            #{build_metrics_cte(&)}
            #{build_final_select}
          ) AS source#{" "}
          ON source.id = target.id AND source.timestamp = target.timestamp
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
        source_table && timestamp_field
      end

      def build_metrics_cte(&)
        <<~SQL
          WITH Metrics AS (
            #{build_metrics_select(&)}
          )
        SQL
      end

      def build_metrics_select(&block)
        <<~SQL
          SELECT
            dimensions.#{id_field} id,
            TIMESTAMP_TRUNC(#{timestamp_field}, #{interval}) timestamp,
            STRUCT(
              #{block.call}
            ) AS metrics
          FROM `#{source_table}`
          WHERE #{timestamp_field} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL #{lookback})
          GROUP BY 1, 2
        SQL
      end

      def build_final_select
        <<~SQL
          SELECT id, timestamp, #{@name}.Dimensions.dimensions, Metrics.metrics#{" "}
          FROM Metrics#{" "}
          LEFT JOIN #{@name}.Dimensions USING (id)
        SQL
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

      def source_table
        @manifold_config&.dig("source", "table")
      end

      def interval
        @manifold_config&.dig("timestamp", "interval") || "DAY"
      end

      def lookback
        @manifold_config&.dig("source", "lookback") || "90 DAY"
      end

      def id_field
        @manifold_config&.dig("source", "id_field") || "id"
      end

      def timestamp_field
        @manifold_config&.dig("timestamp", "field")
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
      attr_writer :merge_config, :manifold_config

      def initialize(name)
        super()
        @name = name
        @vectors = []
        @merge_config = nil
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
        return nil if @vectors.empty? || @merge_config.nil?

        {
          "dataset_id" => name,
          "project" => "${var.project_id}",
          "routine_id" => "merge_dimensions",
          "routine_type" => "PROCEDURE",
          "language" => "SQL",
          "definition_body" => dimensions_merge_routine,
          "depends_on" => ["google_bigquery_dataset.#{name}"]
        }
      end

      def dimensions_merge_routine
        return "" if @vectors.empty? || @merge_config.nil?

        source_sql = File.read(Pathname.pwd.join(@merge_config["source"]))
        SQLBuilder.new(name, @manifold_config).build_dimensions_merge_sql(source_sql)
      end

      def manifold_routine_attributes
        return nil unless valid_manifold_config?

        {
          "dataset_id" => name,
          "project" => "${var.project_id}",
          "routine_id" => "merge_manifold",
          "routine_type" => "PROCEDURE",
          "language" => "SQL",
          "definition_body" => manifold_merge_routine,
          "depends_on" => ["google_bigquery_dataset.#{name}"]
        }
      end

      def manifold_merge_routine
        metrics_builder = MetricsBuilder.new(@manifold_config)
        sql_builder = SQLBuilder.new(name, @manifold_config)
        sql_builder.build_manifold_merge_sql(metrics_builder) do
          metrics_builder.build_metrics_struct
        end
      end

      def valid_manifold_config?
        return false unless @manifold_config

        @manifold_config&.dig("source", "table") &&
          @manifold_config&.dig("timestamp", "field") &&
          @manifold_config&.dig("contexts") &&
          @manifold_config&.dig("metrics")
      end
    end
  end
end
