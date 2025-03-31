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

        # Create metrics subdirectory
        metrics_directory = tables_directory.join("metrics")
        metrics_directory.mkpath

        @manifold_yaml["metrics"].each do |group_name, group_config|
          metrics_table_path = metrics_directory.join("#{group_name}.json")
          metrics_table_schema = metrics_table_schema(group_name, group_config)
          metrics_table_path.write(JSON.pretty_generate(metrics_table_schema).concat("\n"))
          @logger.info("Generated metrics table schema for '#{group_name}'.")
        end
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

        # If there are no breakouts but there are complex logic operations,
        # use the old-style format
        if group_config["breakouts"]&.values&.any? { |v| v.is_a?(Hash) && v["operator"] }
          # Legacy format handling - treat direct keys as breakouts
          return handle_legacy_breakouts(group_config)
        end

        # If no breakouts defined at all, just return conditions/breakouts as is
        unless group_config["breakouts"]
          # Support for older format where breakouts are direct fields
          # Add field for each explicitly defined condition or breakout
          breakout_fields = (group_config.keys - %w[aggregations source filter]).map do |condition_name|
            {
              "name" => condition_name,
              "type" => "RECORD",
              "mode" => "NULLABLE",
              "fields" => breakout_metrics_fields(group_config)
            }
          end
          return breakout_fields
        end

        # Determine conditions list
        conditions = if group_config["conditions"]
                       group_config["conditions"].keys
                     else
                       # If no conditions defined, extract from breakouts
                       extract_conditions_from_breakouts(group_config["breakouts"])
                     end

        # Generate individual condition fields
        condition_fields = generate_condition_fields(conditions, group_config)

        # Generate intersection fields across different breakout groups
        intersection_fields = generate_breakout_intersection_fields(group_config)

        condition_fields + intersection_fields
      end

      def handle_legacy_breakouts(group_config)
        # For legacy format, each key directly in the metrics group is a breakout
        breakout_fields = []

        group_config["breakouts"].each_key do |breakout_name|
          breakout_fields << {
            "name" => breakout_name,
            "type" => "RECORD",
            "mode" => "NULLABLE",
            "fields" => breakout_metrics_fields(group_config)
          }
        end

        breakout_fields
      end

      def extract_conditions_from_breakouts(breakouts)
        # Handle both string and array formats for breakouts
        conditions = []
        breakouts.each do |breakout_name, breakout_values|
          if breakout_values.is_a?(Array)
            conditions.concat(breakout_values)
          elsif breakout_values.is_a?(Hash) && breakout_values["operator"]
            # Skip complex operators in the new format
            next
          else
            # For string format, use the breakout name as the condition
            conditions << breakout_name
          end
        end
        conditions.uniq
      end

      def generate_condition_fields(conditions, group_config)
        # Add a field for each condition
        conditions.map do |condition_name|
          {
            "name" => condition_name,
            "type" => "RECORD",
            "mode" => "NULLABLE",
            "fields" => breakout_metrics_fields(group_config)
          }
        end
      end

      def generate_breakout_intersection_fields(group_config)
        return [] unless group_config["breakouts"]

        intersection_fields = []
        breakout_groups = group_config["breakouts"].keys

        # Skip if there's only one breakout group or if using legacy format
        return [] if breakout_groups.size <= 1 ||
                     group_config["breakouts"].values.any? { |v| v.is_a?(Hash) && v["operator"] }

        # Generate all possible combinations of conditions from different breakout groups
        breakout_groups.combination(2).each do |breakout_pair|
          # Get the conditions in each breakout group
          first_group = breakout_pair[0]
          second_group = breakout_pair[1]

          first_group_conditions = get_breakout_conditions(group_config["breakouts"], first_group)
          second_group_conditions = get_breakout_conditions(group_config["breakouts"], second_group)

          # For each pair of conditions from different breakout groups, create an intersection field
          first_group_conditions.each do |first_condition|
            second_group_conditions.each do |second_condition|
              # Format the intersection name with the second condition capitalized
              intersection_name = "#{first_condition}#{second_condition.capitalize}"

              intersection_fields << {
                "name" => intersection_name,
                "type" => "RECORD",
                "mode" => "NULLABLE",
                "fields" => breakout_metrics_fields(group_config)
              }
            end
          end
        end

        intersection_fields
      end

      def get_breakout_conditions(breakouts, breakout_name)
        breakout_value = breakouts[breakout_name]

        if breakout_value.is_a?(Array)
          # New format: breakout contains array of conditions
          breakout_value
        elsif breakout_value.is_a?(Hash) && breakout_value["operator"]
          # Complex operator breakout - skip for now
          []
        else
          # Legacy format: breakout name is the condition
          [breakout_name]
        end
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
