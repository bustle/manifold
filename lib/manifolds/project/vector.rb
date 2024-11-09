# frozen_string_literal: true

module Manifolds
  module API
    # Describes the entities for whom metrics are calculated.
    class Vector
      attr_reader :name, :project, :config_template_path

      DEFAULT_CONFIG_TEMPLATE_PATH = Pathname.pwd.join(
        "lib", "manifolds", "templates", "vector_template.yml"
      ).freeze

      def initialize(name, project:, config_template_path: DEFAULT_CONFIG_TEMPLATE_PATH)
        self.name = name
        self.project = project
        self.config_template_path = Pathname(config_template_path)
      end

      def add
        directory.mkpath
        FileUtils.cp(config_template_path, config_file_path)
      end

      private

      attr_writer :name, :project, :config_template_path

      def directory
        project.directory.join("vectors")
      end

      def config_file_path
        directory.join("#{name.downcase}.yml")
      end
    end
  end
end
