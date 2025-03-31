# frozen_string_literal: true

module Manifold
  module API
    # Handles schema generation and writing for Manifold tables
    # rubocop:disable Metrics/ClassLength
    class SchemaManager
      def initialize(name, vectors, vector_service, manifold_yaml, logger)
        @name = name
        @vectors = vectors
        @vector_service = vector_service
        @manifold_yaml = manifold_yaml
        @logger = logger
      end

      # Generates and writes schemas to the specified directory
      def write_schemas(tables_directory)
        tables_directory.mkpath
        write_dimensions_schema(tables_directory)
        write_manifold_schema(tables_directory)
        write_metrics_schemas(tables_directory)
      end

      # Returns the dimensions schema structure
      def dimensions_schema
        [
          { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" },
          { "type" => "RECORD", "name" => "dimensions", "mode" => "REQUIRED",
            "fields" => dimensions_fields }
        ]
      end

      # Returns the manifold schema structure
      def manifold_schema
        [
          { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" },
          { "type" => "TIMESTAMP", "name" => "timestamp", "mode" => "REQUIRED" },
          { "type" => "RECORD", "name" => "dimensions", "mode" => "REQUIRED",
            "fields" => dimensions_fields },
          { "type" => "RECORD", "name" => "metrics", "mode" => "REQUIRED",
            "fields" => metrics_fields }
        ]
      end

      private

      def write_dimensions_schema(tables_directory)
        dimensions_path = tables_directory.join("dimensions.json")
        dimensions_path.write(dimensions_schema_json.concat("\n"))
      end

      def write_manifold_schema(tables_directory)
        manifold_path = tables_directory.join("manifold.json")
        manifold_path.write(manifold_schema_json.concat("\n"))
      end

      def write_metrics_schemas(tables_directory)
        return unless @manifold_yaml["metrics"]

        create_metrics_directory(tables_directory)
        write_individual_metrics_schemas(tables_directory)
      end

      def create_metrics_directory(tables_directory)
        metrics_directory = tables_directory.join("metrics")
        metrics_directory.mkpath
      end

      def write_individual_metrics_schemas(tables_directory)
        @manifold_yaml["metrics"].each do |group_name, group_config|
          write_metrics_group_schema(tables_directory, group_name, group_config)
        end
      end

      def write_metrics_group_schema(tables_directory, group_name, group_config)
        metrics_table_path = tables_directory.join("metrics", "#{group_name}.json")
        metrics_table_schema = metrics_table_schema(group_name, group_config)
        metrics_table_path.write(JSON.pretty_generate(metrics_table_schema).concat("\n"))
        @logger.info("Generated metrics table schema for '#{group_name}'.")
      end

      def metrics_table_schema(group_name, group_config)
        [
          { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" },
          { "type" => "TIMESTAMP", "name" => "timestamp", "mode" => "REQUIRED" },
          { "type" => "RECORD", "name" => "metrics", "mode" => "REQUIRED",
            "fields" => [metrics_group_field(group_name, group_config)] }
        ]
      end

      def metrics_group_field(group_name, group_config)
        {
          "name" => group_name,
          "type" => "RECORD",
          "mode" => "NULLABLE",
          "fields" => group_metrics_fields(group_config)
        }
      end

      def dimensions_fields
        @dimensions_fields ||= @vectors.filter_map do |vector|
          @logger.info("Loading vector schema for '#{vector}'.")
          @vector_service.load_vector_schema(vector)
        end
      end

      def dimensions_schema_json
        JSON.pretty_generate(dimensions_schema)
      end

      def manifold_schema_json
        JSON.pretty_generate(manifold_schema)
      end

      def metrics_fields
        return [] unless @manifold_yaml["metrics"]

        @manifold_yaml["metrics"].map do |group_name, group_config|
          {
            "name" => group_name,
            "type" => "RECORD",
            "mode" => "NULLABLE",
            "fields" => group_metrics_fields(group_config)
          }
        end
      end

      def group_metrics_fields(group_config)
        return [] unless group_config["aggregations"]

        # Generate condition fields
        condition_fields = generate_condition_fields(get_conditions_list(group_config), group_config)

        # Generate intersection fields between breakout groups
        intersection_fields = generate_breakout_intersection_fields(group_config)

        condition_fields + intersection_fields
      end

      def get_conditions_list(group_config)
        return [] unless group_config["conditions"]

        group_config["conditions"].keys
      end

      def create_metric_field(field_name, group_config)
        {
          "name" => field_name,
          "type" => "RECORD",
          "mode" => "NULLABLE",
          "fields" => breakout_metrics_fields(group_config)
        }
      end

      def generate_condition_fields(conditions, group_config)
        conditions.map do |condition_name|
          create_metric_field(condition_name, group_config)
        end
      end

      def generate_breakout_intersection_fields(group_config)
        return [] unless group_config["breakouts"]
        return [] if group_config["breakouts"].keys.size <= 1

        generate_all_breakout_combinations(group_config)
      end

      def generate_all_breakout_combinations(group_config)
        all_intersection_fields = []
        breakout_groups = group_config["breakouts"].keys

        # Generate combinations of different sizes (2 to n breakout groups)
        (2..breakout_groups.size).each do |combination_size|
          add_combinations_of_size(combination_size, breakout_groups, group_config, all_intersection_fields)
        end

        all_intersection_fields
      end

      def add_combinations_of_size(size, breakout_groups, group_config, all_fields)
        breakout_groups.combination(size).each do |breakout_combination|
          fields = generate_intersection_fields_for_combination(group_config, breakout_combination)
          all_fields.concat(fields)
        end
      end

      def generate_intersection_fields_for_combination(group_config, breakout_combination)
        # Get all conditions from the given breakout groups
        condition_sets = breakout_combination.map do |breakout_group|
          group_config["breakouts"][breakout_group]
        end

        # Generate all combinations of one condition from each breakout group
        generate_all_condition_combinations(condition_sets, group_config)
      end

      def generate_all_condition_combinations(condition_sets, group_config)
        # Start with first breakout group's conditions
        combinations = condition_sets.first.map { |condition| [condition] }

        # Extend combinations with remaining breakout groups
        extended_combinations = extend_combinations_with_remaining_sets(combinations, condition_sets[1..])

        # Convert combinations to field definitions
        create_intersection_fields(extended_combinations, group_config)
      end

      def extend_combinations_with_remaining_sets(initial_combinations, remaining_sets)
        combinations = initial_combinations

        remaining_sets.each do |conditions|
          combinations = extend_combinations_with_conditions(combinations, conditions)
        end

        combinations
      end

      def extend_combinations_with_conditions(existing_combinations, conditions)
        new_combinations = []

        existing_combinations.each do |existing_combination|
          conditions.each do |condition|
            new_combinations << (existing_combination + [condition])
          end
        end

        new_combinations
      end

      def create_intersection_fields(combinations, group_config)
        combinations.map do |condition_combination|
          # Format name with first condition lowercase, others capitalized
          field_name = format_intersection_name(condition_combination)
          create_metric_field(field_name, group_config)
        end
      end

      def format_intersection_name(condition_combination)
        name = condition_combination.first
        condition_combination[1..].each do |condition|
          name += condition.capitalize
        end
        name
      end

      def breakout_metrics_fields(group_config)
        [
          *countif_fields(group_config),
          *sumif_fields(group_config)
        ]
      end

      def countif_fields(group_config)
        return [] unless group_config.dig("aggregations", "countif")

        [{
          "name" => group_config["aggregations"]["countif"],
          "type" => "INTEGER",
          "mode" => "NULLABLE"
        }]
      end

      def sumif_fields(group_config)
        return [] unless group_config.dig("aggregations", "sumif")

        group_config["aggregations"]["sumif"].keys.map do |metric_name|
          {
            "name" => metric_name,
            "type" => "INTEGER",
            "mode" => "NULLABLE"
          }
        end
      end
    end
    # rubocop:enable Metrics/ClassLength
  end
end
