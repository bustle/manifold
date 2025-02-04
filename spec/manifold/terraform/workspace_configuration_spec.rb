# frozen_string_literal: true

RSpec.describe Manifold::Terraform::WorkspaceConfiguration do
  include FakeFS::SpecHelpers

  subject(:config) { described_class.new(name) }

  let(:name) { "analytics" }

  describe "#as_json" do
    subject(:json) { config.as_json }

    it "includes project_id variable" do
      expect(json["variable"]["project_id"]).to include(
        "description" => "The GCP project ID where resources will be created",
        "type" => "string"
      )
    end

    it "includes dataset configuration" do
      expect(json["resource"]["google_bigquery_dataset"]).to include(
        name => expected_dataset
      )
    end

    it "includes dimensions table configuration" do
      expect(json["resource"]["google_bigquery_table"]).to include(
        "dimensions" => expected_dimensions_table
      )
    end

    context "when vectors have merge configurations" do
      let(:source_sql) { "SELECT id, STRUCT(url, title) AS dimensions FROM pages" }
      let(:vector_config) do
        {
          "name" => "Page",
          "attributes" => {
            "url" => "string",
            "title" => "string"
          },
          "merge" => {
            "source" => "lib/routines/select_pages.sql"
          }
        }
      end

      before do
        Pathname.pwd.join("lib/routines").mkpath
        Pathname.pwd.join("lib/routines/select_pages.sql").write(source_sql)
        config.add_vector(vector_config)
      end

      it "includes routine configuration" do
        expect(json["resource"]["google_bigquery_routine"]).to include(
          "merge_page_dimensions" => {
            "dataset_id" => name,
            "project" => "${var.project_id}",
            "routine_id" => "merge_page_dimensions",
            "routine_type" => "PROCEDURE",
            "language" => "SQL",
            "definition_body" => expected_merge_routine,
            "depends_on" => ["google_bigquery_dataset.#{name}"]
          }
        )
      end

      def expected_merge_routine
        <<~SQL
          MERGE #{name}.Dimensions AS TARGET
          USING (
            #{source_sql}
          ) AS source
          ON source.id = target.id
          WHEN MATCHED THEN UPDATE SET target.page = source.dimensions
          WHEN NOT MATCHED THEN INSERT ROW;
        SQL
      end
    end

    context "when vectors have no merge configurations" do
      let(:vector_config) do
        {
          "name" => "Page",
          "attributes" => {
            "url" => "string",
            "title" => "string"
          }
        }
      end

      before { config.add_vector(vector_config) }

      it "excludes routine configuration" do
        expect(json["resource"]["google_bigquery_routine"]).to be_nil
      end
    end

    def expected_dataset
      {
        "dataset_id" => name,
        "project" => "${var.project_id}",
        "location" => "US"
      }
    end

    def expected_dimensions_table
      {
        "dataset_id" => name,
        "project" => "${var.project_id}",
        "table_id" => "Dimensions",
        "schema" => "${file(\"${path.module}/tables/dimensions.json\")}",
        "depends_on" => ["google_bigquery_dataset.#{name}"]
      }
    end
  end
end
