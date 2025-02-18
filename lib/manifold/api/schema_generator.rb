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
        return [] unless @manifold_yaml["breakouts"] && @manifold_yaml["aggregations"]

        @manifold_yaml["breakouts"].map do |breakout_name, _breakout_config|
          {
            "name" => breakout_name,
            "type" => "RECORD",
            "mode" => "NULLABLE",
            "fields" => breakout_metrics_fields
          }
        end
      end

      def breakout_metrics_fields
        [
          *countif_fields,
          *sumif_fields
        ]
      end

      def countif_fields
        return [] unless @manifold_yaml.dig("aggregations", "countif")

        [{
          "name" => @manifold_yaml["aggregations"]["countif"],
          "type" => "INTEGER",
          "mode" => "NULLABLE"
        }]
      end

      def sumif_fields
        return [] unless @manifold_yaml.dig("aggregations", "sumif")

        @manifold_yaml["aggregations"]["sumif"].keys.map do |metric_name|
          {
            "name" => metric_name,
            "type" => "INTEGER",
            "mode" => "NULLABLE"
          }
        end
      end

      def validate_operator!(operator)
        return if VALID_OPERATORS.include?(operator)

        raise ArgumentError, "Invalid operator: #{operator}. Valid operators are: #{VALID_OPERATORS.join(", ")}"
      end
    end
  end
end
