# frozen_string_literal: true

module Manifold
  module Terraform
    # Represents a Terraform configuration for a Manifold workspace.
    class WorkspaceConfiguration < Configuration
      attr_reader :name

      def initialize(name)
        super()
        @name = name
      end

      def as_json
        {
          "variable" => variables_block,
          "resource" => {
            "google_bigquery_dataset" => dataset_config,
            "google_bigquery_table" => table_config
          }
        }
      end

      private

      def variables_block
        {
          "PROJECT_ID" => {
            "description" => "The GCP project ID where resources will be created",
            "type" => "string"
          }
        }
      end

      def dataset_config
        {
          name => {
            "dataset_id" => name,
            "project" => "${var.PROJECT_ID}",
            "location" => "US"
          }
        }
      end

      def table_config
        {
          "dimensions" => {
            "dataset_id" => name,
            "project" => "${var.PROJECT_ID}",
            "table_id" => "dimensions",
            "schema" => "${file(\"${path.module}/tables/dimensions.json\")}",
            "depends_on" => ["google_bigquery_dataset.#{name}"]
          }
        }
      end
    end
  end
end