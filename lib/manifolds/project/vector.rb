# frozen_string_literal: true

module Manifolds
  module API
    # Describes the entities for whom metrics are calculated.
    class Vector
      attr_reader :name, :project, :template_path

      DEFAULT_TEMPLATE_PATH = Pathname.pwd.join(
        "lib", "manifolds", "templates", "vector_template.yml"
      ).freeze

      def initialize(name, project:, template_path: DEFAULT_TEMPLATE_PATH)
        self.name = name
        self.project = project
        self.template_path = Pathname(template_path)
      end

      def add
        directory.mkpath
        FileUtils.cp(template_path, config_path)
      end

      private

      attr_writer :name, :project, :template_path

      def directory
        project.directory.join("vectors")
      end

      def config_path
        directory.join("#{name.downcase}.yml")
      end
    end
  end
end
