# frozen_string_literal: true

module Manifold
  module Terraform
    # Handles building SQL for manifold routines
    class SQLBuilder
      def initialize(name, manifold_config)
        @name = name
        @manifold_config = manifold_config
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
        timestamp_field && @manifold_config["metrics"] && !@manifold_config["metrics"].empty?
      end

      def timestamp_field
        @manifold_config&.dig("timestamp", "field")
      end

      def metrics_table_name(group_name)
        "#{group_name.capitalize}Metrics"
      end

      def build_source_query
        <<~SQL
          WITH Metrics AS (
            #{build_metrics_select}
          )

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

      # Metrics SQL building methods
      def build_metrics_select
        <<~SQL
          SELECT
              id,
              timestamp,
              #{build_metrics_struct}
            FROM #{build_metric_joins}
        SQL
      end

      def build_metrics_struct
        metric_groups = @manifold_config["metrics"].keys
        metric_groups.map { |group| "#{group}.metrics #{group}" }.join(",\n    ")
      end

      def build_metric_joins
        metric_groups = @manifold_config["metrics"]
        joins = metric_groups.map do |group, config|
          table = "#{@name}.#{metrics_table_name(group)}"
          filter = config["filter"] ? " WHERE #{config["filter"]}" : ""
          "(SELECT * FROM #{table}#{filter}) AS #{group}"
        end
        first = joins.shift
        return first if joins.empty?

        "#{first}\n  #{joins.map { |table| "FULL OUTER JOIN #{table} USING (id, timestamp)" }.join("\n  ")}"
      end
    end

    # Handles building table configurations
    class TableConfigBuilder
      def initialize(name, manifold_config = nil)
        @name = name
        @manifold_config = manifold_config
      end

      def build_table_configs
        configs = {
          "dimensions" => dimensions_table_config,
          "manifold" => manifold_table_config
        }

        if @manifold_config&.dig("metrics")
          @manifold_config["metrics"].each_key do |group_name|
            configs[metrics_table_name(group_name).downcase] = metrics_table_config(group_name)
          end
        end

        configs
      end

      private

      def metrics_table_name(group_name)
        "#{group_name.capitalize}Metrics"
      end

      def dimensions_table_config
        build_table_config("Dimensions")
      end

      def manifold_table_config
        build_table_config("Manifold")
      end

      def metrics_table_config(group_name)
        titlecased_name = metrics_table_name(group_name)
        build_table_config(titlecased_name, "metrics/#{group_name}.json")
      end

      def build_table_config(table_id, schema_path = nil)
        schema_path ||= "#{table_id.downcase}.json"
        config = {
          "dataset_id" => @name,
          "project" => "${var.project_id}",
          "table_id" => table_id,
          "schema" => "${file(\"${path.module}/tables/#{schema_path}\")}",
          "depends_on" => ["google_bigquery_dataset.#{@name}"]
        }

        maybe_apply_partitioning(config, table_id)
      end

      def maybe_apply_partitioning(config, table_id)
        if @manifold_config&.dig("partitioning", "interval") && table_id != "Dimensions"
          interval = @manifold_config["partitioning"]["interval"]
          config["time_partitioning"] = {
            "type" => interval,
            "field" => "timestamp"
          }
        end

        config
      end
    end

    # Represents a Terraform configuration for a Manifold workspace.
    class WorkspaceConfiguration < Configuration # rubocop:disable Metrics/ClassLength
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
            "google_bigquery_table" => TableConfigBuilder.new(name, @manifold_config).build_table_configs,
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
        # add user-defined condition routines, if any
        conds = build_condition_routines
        routines.merge!(conds) unless conds.empty?
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

      # generate scalar function routines for defined conditions
      def build_condition_routines
        return {} unless @manifold_config.is_a?(Hash) && @manifold_config["metrics"].is_a?(Hash)

        @manifold_config["metrics"].each_with_object({}) do |(_grp, grp_cfg), routines|
          next unless grp_cfg.is_a?(Hash) && grp_cfg["conditions"].is_a?(Hash)

          grp_cfg["conditions"].each do |name, cfg|
            id = build_routine_id(name)
            attrs = condition_routine_attributes(id, cfg)
            routines[id] = attrs
          end
        end
      end

      # build attributes for a single condition routine
      def condition_routine_attributes(routine_id, cond_cfg)
        attrs = {
          "dataset_id" => name, "project" => "${var.project_id}", "routine_id" => routine_id,
          "routine_type" => "SCALAR_FUNCTION", "language" => "SQL",
          "definition_body" => cond_cfg["body"], "depends_on" => ["google_bigquery_dataset.#{name}"]
        }
        args = build_arguments(cond_cfg)
        attrs["arguments"] = args if args
        attrs["return_type"] = default_return_type
        attrs
      end

      # helper to extract argument blocks from condition config
      def build_arguments(cond_cfg)
        return unless cond_cfg["args"].is_a?(Hash) && !cond_cfg["args"].empty?

        cond_cfg["args"].map do |arg_name, arg_type|
          { "name" => arg_name, "data_type" => [{ "type_kind" => arg_type }] }
        end
      end

      # default return type for condition routines
      def default_return_type
        [{ "data_type" => [{ "type_kind" => "BOOL" }] }]
      end

      # helper to build routine identifier from condition name
      def build_routine_id(cond_name)
        "is#{cond_name.to_s.split(/_|\\s+/).map(&:capitalize).join}"
      end
    end
  end
end
