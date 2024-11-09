# frozen_string_literal: true

module Manifolds
  module API
    # Projects API
    class Project
      attr_reader :name, :directory

      def initialize(name, directory: Pathname.pwd.join(name))
        self.name = name
        self.directory = Pathname(directory)
      end

      def init
        [workspaces_directory, vectors_directory].each(&:mkpath)
      end

      def workspaces_directory
        directory.join("workspaces")
      end

      def vectors_directory
        directory.join("vectors")
      end

      private

      attr_writer :name, :directory
    end
  end
end
