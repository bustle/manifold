# frozen_string_literal: true

module Manifold
  module API
    # Projects API
    class Project
      attr_reader :name, :logger, :directory

      def initialize(name, logger: Logger.new($stdout), directory: Pathname.pwd.join(name))
        self.name = name
        self.logger = logger
        self.directory = Pathname(directory)
      end

      def self.create(name, directory: Pathname.pwd.join(name))
        new(name, directory: directory).tap do |project|
          [project.workspaces_directory, project.vectors_directory].each(&:mkpath)
        end
      end

      def generate
        workspaces.each(&:generate)
      end

      def workspaces_directory
        directory.join("workspaces")
      end

      def vectors_directory
        directory.join("vectors")
      end

      private

      def workspaces
        @workspaces ||= workspace_directories.map { |dir| Workspace.from_directory(dir, logger: logger) }
      end

      def workspace_directories
        workspaces_directory.children.select(&:directory?)
      end

      attr_writer :name, :logger, :directory
    end
  end
end
