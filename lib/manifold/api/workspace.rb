# frozen_string_literal: true

module Manifold
  module API
    # Encapsulates a single manifold.
    class Workspace
      attr_reader :name, :template_path, :logger

      DEFAULT_TEMPLATE_PATH = File.expand_path(
        "../templates/workspace_template.yml", __dir__
      ).freeze

      def initialize(name, template_path: DEFAULT_TEMPLATE_PATH, logger: Logger.new($stdout))
        @name = name
        @template_path = template_path
        @logger = logger
        @vector_service = Services::VectorService.new(logger)
      end

      def self.from_directory(directory, logger: Logger.new($stdout))
        new(directory.basename.to_s, logger:)
      end

      def add
        [tables_directory, routines_directory].each(&:mkpath)
        FileUtils.cp(template_path, manifold_path)
      end

      def generate(with_terraform: false)
        return unless manifold_exists? && any_vectors?

        generate_dimensions
        generate_terraform if with_terraform
        logger.info("Generated BigQuery dimensions table schema for workspace '#{name}'.")
      end

      def tables_directory
        directory.join("tables")
      end

      def routines_directory
        directory.join("routines")
      end

      def manifold_file
        return nil unless manifold_exists?

        File.new(manifold_path)
      end

      def manifold_exists?
        manifold_path.file?
      end

      def manifold_path
        directory.join("manifold.yml")
      end

      def terraform_main_path
        directory.join("main.tf.json")
      end

      private

      def directory
        Pathname.pwd.join("workspaces", name)
      end

      def manifold_yaml
        @manifold_yaml ||= YAML.safe_load_file(manifold_path)
      end

      def generate_dimensions
        dimensions_path.write(dimensions_schema_json.concat("\n"))
      end

      def dimensions_schema
        [
          { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" },
          { "type" => "RECORD", "name" => "dimensions", "mode" => "REQUIRED",
            "fields" => dimensions_fields }
        ]
      end

      def dimensions_fields
        vectors.filter_map do |vector|
          logger.info("Loading vector schema for '#{vector}'.")
          @vector_service.load_vector_schema(vector)
        end
      end

      def dimensions_schema_json
        JSON.pretty_generate(dimensions_schema)
      end

      def dimensions_path
        tables_directory.join("dimensions.json")
      end

      def any_vectors?
        !(vectors.nil? || vectors.empty?)
      end

      def vectors
        manifold_yaml["vectors"]
      end

      def generate_terraform
        config = Terraform::WorkspaceConfiguration.new(name)
        vectors.each do |vector|
          vector_config = @vector_service.load_vector_config(vector)
          config.add_vector(vector_config)
        end
        config.write(terraform_main_path)
      end
    end
  end
end
