# frozen_string_literal: true

module Manifolds
  module API
    # Encapsulates a single manifold.
    class Workspace
      attr_reader :name, :project, :template_path

      DEFAULT_TEMPLATE_PATH = Pathname.pwd.join(
        "lib", "manifolds", "templates", "workspace_template.yml"
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
        project.workspaces_directory.join(name, "tables")
      end

      def routines_directory
        project.workspaces_directory.join(name, "routines")
      end

      def manifold_file
        return nil unless manifold_exists?

        File.new(manifold_path)
      end

      def manifold_exists?
        manifold_path.file?
      end

      def manifold_path
        project.workspaces_directory.join(name, "manifold.yml")
      end

      private

      attr_writer :name, :project, :template_path
    end
  end
end
