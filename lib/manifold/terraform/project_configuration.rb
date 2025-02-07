# frozen_string_literal: true

module Manifold
  module Terraform
    # Represents a Terraform configuration for a Manifold project.
    class ProjectConfiguration < Configuration
      attr_reader :workspaces, :provider_version, :skip_provider_config

      DEFAULT_TERRAFORM_GOOGLE_PROVIDER_VERSION = "6.18.1"

      def initialize(workspaces, provider_version: DEFAULT_TERRAFORM_GOOGLE_PROVIDER_VERSION,
                     skip_provider_config: false)
        super()
        @workspaces = workspaces
        @provider_version = provider_version
        @skip_provider_config = skip_provider_config
      end

      def as_json
        config = {}
        unless skip_provider_config
          config["terraform"] = terraform_block
          config["provider"] = provider_block
        end

        config.merge!(
          "variable" => variables_block,
          "module" => workspace_modules
        )
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
