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

    it "includes manifold table configuration" do
      expect(json["resource"]["google_bigquery_table"]).to include(
        "manifold" => expected_manifold_table
      )
    end

    context "when vectors have merge configurations" do
      let(:source_sql) { "SELECT id, STRUCT(url, title) AS dimensions FROM pages" }

      before do
        setup_merge_vector_config
        config.add_vector(vector_config)
        config.merge_config = { "source" => "lib/routines/select_pages.sql" }
      end

      it "includes routine configuration" do
        expect(json["resource"]["google_bigquery_routine"]).to include(
          "merge_dimensions" => expected_routine_config
        )
      end
    end

    context "when vectors have no merge configurations" do
      before { config.add_vector(vector_config_without_merge) }

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

    def expected_manifold_table
      {
        "dataset_id" => name,
        "project" => "${var.project_id}",
        "table_id" => "Manifold",
        "schema" => "${file(\"${path.module}/tables/manifold.json\")}",
        "depends_on" => ["google_bigquery_dataset.#{name}"]
      }
    end

    def expected_routine_config
      {
        "dataset_id" => name,
        "project" => "${var.project_id}",
        "routine_id" => "merge_dimensions",
        "routine_type" => "PROCEDURE",
        "language" => "SQL",
        "definition_body" => expected_merge_routine,
        "depends_on" => ["google_bigquery_dataset.#{name}"]
      }
    end

    def expected_merge_routine
      <<~SQL
        MERGE #{name}.Dimensions AS TARGET
        USING (
          #{source_sql}
        ) AS source
        ON source.id = target.id
        WHEN MATCHED THEN UPDATE SET target.dimensions = source.dimensions
        WHEN NOT MATCHED THEN INSERT ROW;
      SQL
    end

    def setup_merge_vector_config
      Pathname.pwd.join("lib/routines").mkpath
      Pathname.pwd.join("lib/routines/select_pages.sql").write(source_sql)
    end

    def vector_config
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

    def vector_config_without_merge
      vector_config.tap { |config| config.delete("merge") }
    end
  end
end
