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
        generate_terraform_entrypoint
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

      def generate_terraform_entrypoint
        terraform_config = {
          "terraform" => {
            "required_providers" => {
              "google" => {
                "source" => "hashicorp/google",
                "version" => "~> 4.0"
              }
            }
          },
          "provider" => {
            "google" => {
              "project" => "${var.project_id}"
            }
          },
          "variable" => {
            "project_id" => {
              "description" => "The GCP project ID where resources will be created",
              "type" => "string"
            }
          },
          "module" => generate_workspace_modules
        }

        terraform_path.write(JSON.pretty_generate(terraform_config))
      end

      def generate_workspace_modules
        workspaces.each_with_object({}) do |workspace, modules|
          modules[workspace.name] = {
            "source" => "./workspaces/#{workspace.name}",
            "project_id" => "${var.project_id}"
          }
        end
      end

      def terraform_path
        directory.join("main.tf.json")
      end
    end
  end
end
