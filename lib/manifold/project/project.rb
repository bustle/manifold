# frozen_string_literal: true

module Manifold
  module API
    # Projects API
    class Project
      DEFAULT_CONFIG = {
        name: File.basename(Dir.getwd)
      }

      attr_reader :config_path

      def initialize(config: Pathname.pwd.join("project.yaml"))
        self.config_path = config
      end

      def create
        File.open(config_path, "w") { |file| file.write DEFAULT_CONFIG.to_yaml }
        [workspaces_directory, vectors_directory].each(&:mkpath)
      end

      def directory
        Pathname.new(Dir.pwd)
      end

      def workspaces_directory
        directory.join("workspaces")
      end

      def vectors_directory
        directory.join("vectors")
      end

      def created?
        File.exist? config_path
      end

      def config
        return nil unless created?

        @config ||= YAML.safe_load_file(config_path, permitted_classes: [Symbol])
      end

      private

      attr_writer :config_path
    end
  end
end
