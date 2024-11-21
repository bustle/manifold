# frozen_string_literal: true

module Manifold
  module API
    # Projects API
    class Project
      attr_reader :name, :logger, :directory

      def initialize(name, logger: Logger.new($stdout), directory: Pathname.pwd.join(name))
        @name = name
        @logger = logger
        @directory = Pathname(directory)
      end

      def self.create(name, directory: Pathname.pwd.join(name))
        new(name, directory:).tap do |project|
          [project.workspaces_directory, project.vectors_directory].each(&:mkpath)
        end
      end

      def workspaces
        @workspaces ||= workspace_directories.map { |dir| Workspace.from_directory(dir, logger:) }
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

      def workspace_directories
        workspaces_directory.children.select(&:directory?)
      end
    end
  end
end
