# frozen_string_literal: true

module Manifold
  module API
    # Handles terraform configuration generation
    class TerraformGenerator
      attr_accessor :manifold_config

      def initialize(name, vectors, vector_service, manifold_yaml)
        @name = name
        @vectors = vectors
        @vector_service = vector_service
        @manifold_yaml = manifold_yaml
      end

      def generate(path)
        config = Terraform::WorkspaceConfiguration.new(@name)
        @vectors.each do |vector|
          vector_config = @vector_service.load_vector_config(vector)
          config.add_vector(vector_config)
        end
        config.dimensions_config = @manifold_yaml["dimensions"]&.fetch("merge", nil) if @manifold_yaml["dimensions"]
        config.manifold_config = @manifold_yaml
        config.write(path)
      end
    end

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
        return nil unless manifold_exists? && any_vectors?

        write_schemas
        logger.info("Generated BigQuery dimensions table schema for workspace '#{name}'.")

        return unless with_terraform

        write_manifold_merge_sql
        write_dimensions_merge_sql
        write_metrics_merge_sql
        generate_terraform
        logger.info("Generated Terraform configuration for workspace '#{name}'.")
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

      def write_manifold_merge_sql
        return unless manifold_file

        sql_builder = Terraform::SQLBuilder.new(name, manifold_yaml)
        sql = sql_builder.build_manifold_merge_sql
        routines_directory.join("merge_manifold.sql").write(sql)
      end

      def write_dimensions_merge_sql
        return unless valid_dimensions_config?

        source_sql = File.read(Pathname.pwd.join(manifold_yaml["dimensions"]["merge"]["source"]))
        sql_builder = Terraform::SQLBuilder.new(name, manifold_yaml)
        sql = sql_builder.build_dimensions_merge_sql(source_sql)
        return unless sql

        write_dimensions_merge_sql_file(sql)
      end

      def write_metrics_merge_sql
        return unless manifold_file
        @manifold_yaml["metrics"]&.each_key do |group_name|
          sql_builder = Terraform::SQLBuilder.new(name, manifold_yaml)
          sql = sql_builder.build_metric_merge_sql(group_name)
          routines_directory.join("merge_#{group_name}.sql").write(sql)
        end
      end

      def valid_dimensions_config?
        return false unless manifold_yaml

        !manifold_yaml["dimensions"]&.dig("merge", "source").nil?
      end

      def write_dimensions_merge_sql_file(sql)
        routines_directory.mkpath
        dimensions_merge_sql_path.write(sql)
      end

      def dimensions_merge_sql_path
        routines_directory.join("merge_dimensions.sql")
      end

      private

      def directory
        Pathname.pwd.join("workspaces", name)
      end

      def manifold_yaml
        @manifold_yaml ||= YAML.safe_load_file(manifold_path)
      end

      def write_schemas
        SchemaManager.new(name, vectors, @vector_service, manifold_yaml, logger)
                     .write_schemas(tables_directory)
      end

      def any_vectors?
        !(vectors.nil? || vectors.empty?)
      end

      def vectors
        manifold_yaml["vectors"]
      end

      def generate_terraform
        terraform_generator = TerraformGenerator.new(name, vectors, @vector_service, manifold_yaml)
        terraform_generator.manifold_config = manifold_yaml
        terraform_generator.generate(terraform_main_path)
      end
    end
  end
end
