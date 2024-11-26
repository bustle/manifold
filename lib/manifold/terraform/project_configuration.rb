# frozen_string_literal: true

module Manifold
  module Terraform
    # Represents a Terraform configuration for a Manifold project.
    class ProjectConfiguration < Configuration
      attr_reader :workspaces, :provider_version

      DEFAULT_TERRAFORM_GOOGLE_PROVIDER_VERSION = "6.12.0"

      def initialize(workspaces, provider_version: DEFAULT_TERRAFORM_GOOGLE_PROVIDER_VERSION)
        super()
        @workspaces = workspaces
        @provider_version = provider_version
      end

      def as_json
        {
          "terraform" => terraform_block,
          "provider" => provider_block,
          "variable" => variables_block,
          "module" => workspace_modules
        }
      end

      private

      def terraform_block
        {
          "required_providers" => {
            "google" => {
              "source" => "hashicorp/google",
              "version" => provider_version
            }
          }
        }
      end

      def provider_block
        {
          "google" => {
            "project" => "${var.project_id}"
          }
        }
      end

      def variables_block
        {
          "project_id" => {
            "description" => "The GCP project ID where resources will be created",
            "type" => "string"
          }
        }
      end

      def workspace_modules
        workspaces.each_with_object({}) do |workspace, modules|
          modules[workspace.name] = {
            "source" => "./workspaces/#{workspace.name}",
            "project_id" => "${var.project_id}"
          }
        end
      end
    end
  end
end
