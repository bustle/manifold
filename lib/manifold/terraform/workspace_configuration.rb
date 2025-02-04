# frozen_string_literal: true

module Manifold
  module Terraform
    # Represents a Terraform configuration for a Manifold workspace.
    class WorkspaceConfiguration < Configuration
      attr_reader :name

      def initialize(name)
        super()
        @name = name
        @vectors = []
      end

      def add_vector(vector_config)
        @vectors << vector_config
      end

      def as_json
        {
          "variable" => variables_block,
          "resource" => {
            "google_bigquery_dataset" => dataset_config,
            "google_bigquery_table" => table_config,
            "google_bigquery_routine" => routine_config
          }.compact
        }
      end

      private

      def variables_block
        {
          "project_id" => {
            "description" => "The GCP project ID where resources will be created",
            "type" => "string"
          }
        }
      end

      def dataset_config
        {
          name => {
            "dataset_id" => name,
            "project" => "${var.project_id}",
            "location" => "US"
          }
        }
      end

      def table_config
        {
          "dimensions" => {
            "dataset_id" => name,
            "project" => "${var.project_id}",
            "table_id" => "Dimensions",
            "schema" => "${file(\"${path.module}/tables/dimensions.json\")}",
            "depends_on" => ["google_bigquery_dataset.#{name}"]
          }
        }
      end

      def routine_config
        return nil if @vectors.empty?

        routines = @vectors.filter_map { |vector| build_routine(vector) }
        routines.empty? ? nil : routines.to_h
      end

      def build_routine(vector)
        return nil unless vector["merge"]&.fetch("source", nil)

        routine_name = "merge_#{vector["name"].downcase}_dimensions"
        [routine_name, routine_attributes(routine_name, vector)]
      end

      def routine_attributes(routine_name, vector)
        {
          "dataset_id" => name,
          "project" => "${var.project_id}",
          "routine_id" => routine_name,
          "routine_type" => "PROCEDURE",
          "language" => "SQL",
          "definition_body" => merge_routine_definition(vector),
          "depends_on" => ["google_bigquery_dataset.#{name}"]
        }
      end

      def merge_routine_definition(vector)
        source_sql = read_source_sql(vector["merge"]["source"])
        <<~SQL
          MERGE #{name}.Dimensions AS TARGET
          USING (
            #{source_sql}
          ) AS source
          ON source.id = target.id
          WHEN MATCHED THEN UPDATE SET target.#{vector["name"].downcase} = source.dimensions
          WHEN NOT MATCHED THEN INSERT ROW;
        SQL
      end

      def read_source_sql(source_path)
        File.read(Pathname.pwd.join(source_path))
      end
    end
  end
end
