# frozen_string_literal: true

module Manifold
  module API
    # Handles schema generation for Manifold tables
    class SchemaGenerator
      VALID_OPERATORS = %w[AND OR NOT NAND NOR XOR XNOR].freeze

      def initialize(dimensions_fields, manifold_yaml)
        @dimensions_fields = dimensions_fields
        @manifold_yaml = manifold_yaml
      end

      def dimensions_schema
        [
          { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" },
          { "type" => "RECORD", "name" => "dimensions", "mode" => "REQUIRED",
            "fields" => @dimensions_fields }
        ]
      end

      def manifold_schema
        [
          { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" },
          { "type" => "TIMESTAMP", "name" => "timestamp", "mode" => "REQUIRED" },
          { "type" => "RECORD", "name" => "dimensions", "mode" => "REQUIRED",
            "fields" => @dimensions_fields },
          { "type" => "RECORD", "name" => "metrics", "mode" => "REQUIRED",
            "fields" => metrics_fields }
        ]
      end

      private

      def metrics_fields
        return [] unless @manifold_yaml["metrics"]

        @manifold_yaml["metrics"].map do |group_name, group_config|
          {
            "name" => group_name,
            "type" => "RECORD",
            "mode" => "REQUIRED",
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

      def metric_group?(key)
        return false unless @manifold_yaml[key].is_a?(Hash)

        @manifold_yaml[key]["breakouts"] && @manifold_yaml[key]["aggregations"]
      end

      def validate_operator!(operator)
        return if VALID_OPERATORS.include?(operator)

        raise ArgumentError, "Invalid operator: #{operator}. Valid operators are: #{VALID_OPERATORS.join(", ")}"
      end
    end
  end
end
