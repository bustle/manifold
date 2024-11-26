# frozen_string_literal: true

RSpec.describe Manifold::Terraform::WorkspaceConfiguration do
  include FakeFS::SpecHelpers

  subject(:config) { described_class.new(name) }

  let(:name) { "analytics" }

  describe "#as_json" do
    subject(:json) { config.as_json }

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
        "project" => "${var.project_id}",
        "location" => "US"
      }
    end

    def expected_dimensions_table
      {
        "dataset_id" => name,
        "project" => "${var.project_id}",
        "table_id" => "dimensions",
        "schema" => "${file(\"${path.module}/tables/dimensions.json\")}",
        "depends_on" => ["google_bigquery_dataset.#{name}"]
      }
    end
  end
end
