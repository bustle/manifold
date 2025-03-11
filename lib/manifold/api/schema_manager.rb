# frozen_string_literal: true

module Manifold
  module API
    # Handles schema generation and writing for Manifold tables
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
        return [] unless group_config["breakouts"] && group_config["aggregations"]

        group_config["breakouts"].map do |breakout_name, _breakout_config|
          {
            "name" => breakout_name,
            "type" => "RECORD",
            "mode" => "NULLABLE",
            "fields" => breakout_metrics_fields(group_config)
          }
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
  end
end
