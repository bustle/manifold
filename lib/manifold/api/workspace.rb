# frozen_string_literal: true

module Manifold
  module API
    # Encapsulates a single manifold.
    class Workspace
      attr_reader :name, :template_path

      DEFAULT_TEMPLATE_PATH = File.expand_path(
        "../templates/workspace_template.yml", __dir__
      ).freeze

      def initialize(name, template_path: DEFAULT_TEMPLATE_PATH)
        self.name = name
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
        Pathname.pwd.join("workspaces", name)
      end

      attr_writer :name, :template_path
    end
  end
end
