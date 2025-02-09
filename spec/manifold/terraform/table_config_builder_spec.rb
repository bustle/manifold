# frozen_string_literal: true

RSpec.describe Manifold::Terraform::TableConfigBuilder do
  subject(:builder) { described_class.new(name) }

  let(:name) { "analytics" }

  describe "#build_table_configs" do
    subject(:configs) { builder.build_table_configs }

    it "includes both dimensions and manifold tables" do
      expect(configs.keys).to contain_exactly("dimensions", "manifold")
    end

    describe "dimensions table configuration" do
      subject(:dimensions_config) { configs["dimensions"] }

      it { is_expected.to include("dataset_id" => name) }
      it { is_expected.to include("project" => "${var.project_id}") }
      it { is_expected.to include("table_id" => "Dimensions") }
      it { is_expected.to include("schema" => "${file(\"${path.module}/tables/dimensions.json\")}") }
      it { is_expected.to include("depends_on" => ["google_bigquery_dataset.#{name}"]) }
    end

    describe "manifold table configuration" do
      subject(:manifold_config) { configs["manifold"] }

      it { is_expected.to include("dataset_id" => name) }
      it { is_expected.to include("project" => "${var.project_id}") }
      it { is_expected.to include("table_id" => "Manifold") }
      it { is_expected.to include("schema" => "${file(\"${path.module}/tables/manifold.json\")}") }
      it { is_expected.to include("depends_on" => ["google_bigquery_dataset.#{name}"]) }
    end
  end
end
