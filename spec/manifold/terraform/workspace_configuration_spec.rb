# frozen_string_literal: true

RSpec.describe Manifold::Terraform::WorkspaceConfiguration do
  include FakeFS::SpecHelpers

  subject(:config) { described_class.new(name) }

  let(:name) { "analytics" }

  describe "#as_json" do
    subject(:json) { config.as_json }

    it "includes PROJECT_ID variable" do
      expect(json["variable"]["PROJECT_ID"]).to include(
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

    def expected_dataset
      {
        "dataset_id" => name,
        "project" => "${var.PROJECT_ID}",
        "location" => "US"
      }
    end

    def expected_dimensions_table
      {
        "dataset_id" => name,
        "project" => "${var.PROJECT_ID}",
        "table_id" => "dimensions",
        "schema" => "${file(\"${path.module}/tables/dimensions.json\")}",
        "depends_on" => ["google_bigquery_dataset.#{name}"]
      }
    end
  end
end
