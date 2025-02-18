# frozen_string_literal: true

RSpec.describe Manifold::Terraform::WorkspaceConfiguration do
  include FakeFS::SpecHelpers

  subject(:config) { described_class.new(name) }

  let(:name) { "analytics" }
  let(:manifold_config) do
    {
      "timestamp" => {
        "field" => "created_at",
        "interval" => "DAY"
      },
      "metrics" => {
        "taps" => {
          "breakouts" => {
            "paid" => "IS_PAID(context.location)"
          },
          "aggregations" => {
            "countif" => "tapCount"
          },
          "source" => "analytics.events",
          "filter" => "timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)"
        }
      }
    }
  end

  include_context "with template files"

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
      let(:merge_dimensions_routine) { json["resource"]["google_bigquery_routine"]["merge_dimensions"] }
      let(:dimensions_routine_details) do
        {
          definition_body: merge_dimensions_routine["definition_body"],
          sql_content: Pathname.pwd.join("workspaces", name, "routines", "merge_dimensions.sql").read
        }
      end

      before do
        setup_merge_vector_config
        config.add_vector(vector_config)
        config.dimensions_config = { "source" => "lib/routines/select_pages.sql" }

        workspace = Manifold::API::Workspace.new(name)
        workspace.add
        workspace.manifold_path.write(<<~YAML)
          vectors:
            - Page
          dimensions:
            merge:
              source: lib/routines/select_pages.sql
        YAML
        workspace.write_dimensions_merge_sql
      end

      it "includes dimensions merge routine configuration" do
        expect(json["resource"]["google_bigquery_routine"]).to include(
          "merge_dimensions" => expected_routine_config
        )
      end

      it "references the merge SQL file" do
        file_path = "${file(\"${path.module}/routines/merge_dimensions.sql\")}"
        expect(dimensions_routine_details[:definition_body]).to eq(file_path)
      end

      it "includes dataset dependency" do
        expect(merge_dimensions_routine["depends_on"]).to eq(["google_bigquery_dataset.#{name}"])
      end

      it "includes the source SQL in the file" do
        expect(dimensions_routine_details[:sql_content]).to include(source_sql)
      end
    end
  end

  private

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
      "definition_body" => "${file(\"${path.module}/routines/merge_dimensions.sql\")}",
      "depends_on" => ["google_bigquery_dataset.#{name}"]
    }
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
end
