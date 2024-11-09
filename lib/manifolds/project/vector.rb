# frozen_string_literal: true

module Manifolds
  module API
    # Describes the entities for whom metrics are calculated.
    class Vector
      attr_reader :name, :project, :config_template_path

      DEFAULT_CONFIG_TEMPLATE_PATH = File.join(
        Dir.pwd, "lib", "manifolds", "templates", "vector_template.yml"
      )

      def initialize(name, project:, config_template_path: DEFAULT_CONFIG_TEMPLATE_PATH)
        self.name = name
        self.project = project
        self.config_template_path = Pathname(config_template_path)
      end

      def add
        [routines_directory, tables_directory].each(&:mkpath)
        FileUtils.cp(config_template_path, config_file_path)
      end

      def tables_directory
        Pathname.new(File.join(project.vectors_directory, "tables"))
      end

      def routines_directory
        Pathname.new(File.join(project.vectors_directory, "routines"))
      end

      private

      attr_writer :name, :project, :config_template_path

      def config_file_path
        project.directory.join("vectors", config_file_name)
      end

      def config_file_name
        "#{name.downcase}.yml"
      end
    end
  end
end
