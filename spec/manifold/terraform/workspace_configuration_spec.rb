# frozen_string_literal: true

RSpec.describe Manifold::Terraform::WorkspaceConfiguration do
  include FakeFS::SpecHelpers

  subject(:config) { described_class.new(name) }

  let(:name) { "analytics" }
  let(:manifold_config) do
    {
      "contexts" => {
        "paid" => "IS_PAID(context.location)"
      },
      "metrics" => {
        "countif" => "tapCount"
      },
      "source" => "analytics.events",
      "filter" => "timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)",
      "timestamp" => {
        "field" => "created_at",
        "interval" => "DAY"
      }
    }
  end

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

      it "includes dimensions merge routine configuration" do
        expect(json["resource"]["google_bigquery_routine"]).to include(
          "merge_dimensions" => expected_routine_config
        )
      end
    end

    context "when manifold configuration is present" do
      before do
        config.manifold_config = manifold_config
      end

      let(:merge_manifold_routine) { json["resource"]["google_bigquery_routine"]["merge_manifold"] }
      let(:definition_body) { merge_manifold_routine["definition_body"] }

      it "includes merge_manifold routine" do
        expect(json["resource"]["google_bigquery_routine"]).to include("merge_manifold")
      end

      it "configures the dataset" do
        expect(merge_manifold_routine).to include(
          "dataset_id" => name,
          "project" => "${var.project_id}"
        )
      end

      it "configures the routine type" do
        expect(merge_manifold_routine).to include(
          "routine_id" => "merge_manifold",
          "routine_type" => "PROCEDURE",
          "language" => "SQL"
        )
      end

      it "includes the merge SQL" do
        expect(definition_body).to include("MERGE analytics.Manifold AS target")
      end

      it "includes dataset dependency" do
        expect(merge_manifold_routine["depends_on"]).to eq(["google_bigquery_dataset.#{name}"])
      end

      it "uses the configured timestamp field" do
        expect(definition_body).to include("TIMESTAMP_TRUNC(created_at, DAY)")
      end

      it "uses the configured filter" do
        expect(definition_body).to include("WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)")
      end

      it "includes countif metrics" do
        expect(definition_body).to include("COUNTIF(IS_PAID(context.location)) AS tapCount")
      end
    end

    context "when vectors have no merge configurations" do
      before { config.add_vector(vector_config_without_merge) }

      it "excludes routine configuration" do
        expect(json["resource"]["google_bigquery_routine"]).to be_nil
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
