# frozen_string_literal: true

module Manifolds
  module Services
    # Handles the generation of BigQuery schemas based on project configurations
    class BigQueryService
      def initialize(logger)
        @logger = logger
        @vector_service = Manifolds::Services::VectorService.new(logger)
      end

      def generate_dimensions_schema(project_name)
        config_path = File.join(Dir.pwd, "projects", project_name, "manifold.yml")
        return unless validate_config_exists(config_path, project_name)

        config = YAML.safe_load_file(config_path)
        fields = []

        # Load vector schemas
        config["vectors"]&.each do |vector|
          @logger.info("Loading vector schema for '#{vector}'.")
          vector_schema = @vector_service.load_vector_schema(vector)
          fields << vector_schema if vector_schema
        end

        create_dimensions_file(project_name, fields)
      end

      private

      def validate_config_exists(config_path, project_name)
        unless File.exist?(config_path)
          @logger.error("Config file missing for project '#{project_name}'.")
          return false
        end
        true
      end

      def create_dimensions_file(project_name, fields)
        tables_directory(project_name).mkpath
        dimensions = dimensions_schema(fields)

        File.write(dimensions_file(project_name), dimensions)
        @logger.info("Generated BigQuery dimensions table schema for '#{project_name}'.")
      end

      def dimensions_schema(fields)
        JSON.pretty_generate([
                               { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" },
                               { "type" => "RECORD", "name" => "dimensions", "mode" => "REQUIRED",
                                 "fields" => fields }
                             ]).concat("\n")
      end

      def tables_directory(project_name)
        Pathname.pwd.join("projects", project_name, "bq", "tables")
      end

      def dimensions_file(project_name)
        tables_directory(project_name).join("dimensions.json")
      end
    end
  end
end
