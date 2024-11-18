# frozen_string_literal: true

module Manifold
  module API
    # Encapsulates a single manifold.
    class Workspace
      attr_reader :name, :project, :template_path

      DEFAULT_TEMPLATE_PATH = Pathname.pwd.join(
        "lib", "manifold", "templates", "workspace_template.yml"
      )

      def initialize(name, project:, template_path: DEFAULT_TEMPLATE_PATH)
        self.name = name
        self.project = project
        self.template_path = template_path
      end

      def add
        [tables_directory, routines_directory].each(&:mkpath)
        FileUtils.cp(template_path, manifold_path)
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

      private

      def directory
        project.directory.join("workspaces", name)
      end

      attr_writer :name, :project, :template_path
    end
  end
end
