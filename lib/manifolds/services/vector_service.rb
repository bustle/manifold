# frozen_string_literal: true

module Manifold
  module Services
    # Handles the loading of vector schemas from configuration files
    class VectorService
      def initialize(logger)
        @logger = logger
      end

      def load_vector_schema(vector_name)
        path = config_path(vector_name)
        unless path.file?
          @logger.error("Vector configuration not found: #{path}")
          return nil
        end

        config = YAML.safe_load_file(path)
        fields = transform_attributes_to_schema(config["attributes"])
        { "name" => vector_name.downcase, "type" => "RECORD", "fields" => fields }
      end

      private

      def transform_attributes_to_schema(attributes)
        attributes.map do |name, type|
          {
            "name" => name,
            "type" => type.upcase,
            "mode" => "NULLABLE"
          }
        end
      end

      def config_path(vector_name)
        Pathname.pwd.join("vectors", "#{vector_name.downcase}.yml")
      end
    end
  end
end
