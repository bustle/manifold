# frozen_string_literal: true

# rubocop:disable Metrics/MethodLength, Metrics/AbcSize

module Manifold
  module Terraform
    # rubocop:disable Metrics/ClassLength
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

      def build_metric_merge_sql(group_name)
        return "" unless valid_config? && @manifold_config["metrics"][group_name]

        config = @manifold_config["metrics"][group_name]
        ts_field = timestamp_field
        interval = @manifold_config.dig("timestamp", "interval")
        source = config["source"]
        filter_clause = config["filter"] ? " WHERE #{config["filter"]}" : ""
        metrics_struct = build_group_metrics_struct(group_name, config)
        <<~SQL
          MERGE #{@name}.#{metrics_table_name(group_name)} AS target
          USING (
            SELECT
              id,
              TIMESTAMP_TRUNC(#{ts_field}, #{interval}) AS timestamp,
              STRUCT(#{metrics_struct}) AS metrics
            FROM #{source}#{filter_clause}
            GROUP BY id, timestamp
          ) AS source
          ON source.id = target.id AND source.timestamp = target.timestamp
          WHEN MATCHED THEN UPDATE SET metrics = source.metrics
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

      # Builds the inner struct fields for a metrics group
      def build_group_metrics_struct(_group_name, config)
        condition_names = config["conditions"]&.keys || []
        intersection_names = build_intersection_names(config)
        field_names = condition_names + intersection_names
        parts = field_names.map do |name|
          expr = build_condition_expression(name, config)
          aggregates = []
          if config.dig("aggregations", "countif")
            count_name = config["aggregations"]["countif"]
            aggregates << "COUNTIF(#{expr}) AS #{count_name}"
          end
          if config.dig("aggregations", "sumif")
            config["aggregations"]["sumif"].each do |metric, sum_cfg|
              field = sum_cfg["field"]
              aggregates << "SUM(IF(#{expr}, #{field}, 0)) AS #{metric}"
            end
          end
          "STRUCT(#{aggregates.join(", ")}) AS #{name}"
        end
        parts.join(", ")
      end

      # Determines the condition expression, using a scalar function if args exist
      def build_condition_expression(name, config)
        cond_cfg = config["conditions"][name]
        args = cond_cfg["args"]&.keys
        if args && !args.empty?
          "is#{name.capitalize}(#{args.join(", ")})"
        else
          cond_cfg["body"]
        end
      end

      # Builds intersection condition names from breakouts
      # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
      def build_intersection_names(config)
        breakouts = config["breakouts"] || {}
        groups = breakouts.keys
        return [] if groups.size <= 1

        (2..groups.size).flat_map do |size|
          groups.combination(size).flat_map do |combo|
            condition_sets = combo.map { |g| breakouts[g] }
            combos = condition_sets.first.map { |c| [c] }
            combos = condition_sets[1..].reduce(combos) do |acc, set|
              acc.flat_map { |prev| set.map { |c| prev + [c] } }
            end
            combos.map { |conds| format_intersection_name(conds) }
          end
        end
      end
      # rubocop:enable Metrics/AbcSize, Metrics/MethodLength, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

      # Formats intersection names (first lowercase, others capitalized)
      def format_intersection_name(conds)
        name = conds.first
        conds[1..].each { |c| name += c.capitalize }
        name
      end
    end
    # rubocop:enable Metrics/ClassLength

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

      # rubocop:disable Metrics/MethodLength
      def routine_config
        routines = {
          "merge_dimensions" => dimensions_routine_attributes,
          "merge_manifold" => manifold_routine_attributes
        }.compact
        # add metric merge routines
        if @manifold_config&.dig("metrics")
          @manifold_config["metrics"].each_key do |group|
            routines["merge_#{group}"] = metric_routine_attributes(group)
          end
        end
        # add user-defined condition routines, if any
        conds = build_condition_routines
        routines.merge!(conds) unless conds.empty?
        routines.empty? ? nil : routines
      end
      # rubocop:enable Metrics/MethodLength

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

      # Builds attributes for a metric merge routine
      def metric_routine_attributes(group_name)
        {
          "dataset_id" => name,
          "project" => "${var.project_id}",
          "routine_id" => "merge_#{group_name}",
          "routine_type" => "PROCEDURE",
          "language" => "SQL",
          "definition_body" => "${file(\"${path.module}/routines/merge_#{group_name}.sql\")}",
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
