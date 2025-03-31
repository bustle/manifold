# frozen_string_literal: true

RSpec.describe Manifold::API::SchemaManager do
  include FakeFS::SpecHelpers

  subject(:schema_manager) { described_class.new(name, vectors, vector_service, manifold_yaml, logger) }

  let(:logger) { instance_spy(Logger) }
  let(:name) { "test_workspace" }
  let(:vectors) { ["TestVector"] }
  let(:vector_service) { instance_spy(Manifold::Services::VectorService) }
  let(:manifold_yaml) do
    {
      "metrics" => {
        "renders" => {
          "conditions" => {
            "mobile" => "IS_DESKTOP(context.device)",
            "desktop" => "IS_MOBILE(context.device)",
            "us" => "context.geo.country = 'US'",
            "global" => "context.geo.country != 'US'"
          },
          "breakouts" => {
            "device" => %w[mobile desktop],
            "region" => %w[us global]
          },
          "aggregations" => {
            "countif" => "renderCount",
            "sumif" => {
              "sequenceSum" => {
                "field" => "context.sequence"
              }
            }
          }
        }
      }
    }
  end

  before do
    # Mock the vector service
    allow(vector_service).to receive(:load_vector_schema).and_return(
      { "name" => "test_vector", "type" => "STRING", "mode" => "NULLABLE" }
    )
  end

  describe "#dimensions_schema" do
    it "includes required id field" do
      schema = schema_manager.dimensions_schema
      expect(schema).to include(
        { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" }
      )
    end

    it "includes dimensions field with RECORD type" do
      schema = schema_manager.dimensions_schema
      dimensions_field = schema.find { |f| f["name"] == "dimensions" }
      expect(dimensions_field["type"]).to eq("RECORD")
    end

    it "includes dimensions field with REQUIRED mode" do
      schema = schema_manager.dimensions_schema
      dimensions_field = schema.find { |f| f["name"] == "dimensions" }
      expect(dimensions_field["mode"]).to eq("REQUIRED")
    end
  end

  describe "#manifold_schema" do
    it "includes required id field" do
      schema = schema_manager.manifold_schema
      expect(schema).to include(
        { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" }
      )
    end

    it "includes required timestamp field" do
      schema = schema_manager.manifold_schema
      expect(schema).to include(
        { "type" => "TIMESTAMP", "name" => "timestamp", "mode" => "REQUIRED" }
      )
    end

    it "includes dimensions field with RECORD type" do
      schema = schema_manager.manifold_schema
      dimensions_field = schema.find { |f| f["name"] == "dimensions" }
      expect(dimensions_field["type"]).to eq("RECORD")
    end

    it "includes metrics field with RECORD type" do
      schema = schema_manager.manifold_schema
      metrics_field = schema.find { |f| f["name"] == "metrics" }
      expect(metrics_field["type"]).to eq("RECORD")
    end
  end

  describe "#metrics_fields" do
    let(:metrics_fields) { schema_manager.send(:metrics_fields) }
    let(:renders_field) { metrics_fields.find { |field| field["name"] == "renders" } }
    let(:fields) { renders_field["fields"] }
    let(:field_names) { fields.map { |field| field["name"] } }

    it "includes the renders group field" do
      expect(renders_field).not_to be_nil
    end

    it "includes all individual condition fields" do
      expect(field_names).to include("mobile", "desktop", "us", "global")
    end

    it "includes intersection fields between different breakout groups" do
      # We support both naming conventions (first to second or second to first)
      mobile_us_present = field_names.include?("mobileUs") || field_names.include?("usMobile")
      desktop_us_present = field_names.include?("desktopUs") || field_names.include?("usDesktop")
      mobile_global_present = field_names.include?("mobileGlobal") || field_names.include?("globalMobile")
      desktop_global_present = field_names.include?("desktopGlobal") || field_names.include?("globalDesktop")

      expect(mobile_us_present).to be(true)
      expect(desktop_us_present).to be(true)
      expect(mobile_global_present).to be(true)
      expect(desktop_global_present).to be(true)
    end

    it "does not include intersection fields from the same breakout group" do
      # Should not include mobile/desktop combinations (same breakout)
      expect(field_names).not_to include("mobileDesktop")
      expect(field_names).not_to include("desktopMobile")

      # Should not include us/global combinations (same breakout)
      expect(field_names).not_to include("usGlobal")
      expect(field_names).not_to include("globalUs")
    end

    it "includes correct aggregation fields for individual conditions" do
      mobile_field = fields.find { |field| field["name"] == "mobile" }
      expect(mobile_field["fields"].map { |f| f["name"] }).to include("renderCount", "sequenceSum")
    end

    it "includes correct aggregation fields for intersection conditions" do
      # Find an intersection field (using either naming convention)
      intersection_field = fields.find do |field|
        field["name"] == "mobileUs" || field["name"] == "usMobile"
      end

      expect(intersection_field).not_to be_nil
      expect(intersection_field["fields"].map { |f| f["name"] }).to include("renderCount", "sequenceSum")
    end
  end

  context "when conditions are not explicitly defined" do
    let(:manifold_yaml) do
      {
        "metrics" => {
          "renders" => {
            "breakouts" => {
              "device" => %w[mobile desktop],
              "region" => %w[us global]
            },
            "aggregations" => {
              "countif" => "renderCount"
            }
          }
        }
      }
    end

    let(:metrics_fields) { schema_manager.send(:metrics_fields) }
    let(:renders_field) { metrics_fields.find { |field| field["name"] == "renders" } }
    let(:fields) { renders_field["fields"] }
    let(:field_names) { fields.map { |field| field["name"] } }

    it "derives condition fields from breakouts" do
      expect(field_names).to include("mobile", "desktop", "us", "global")
    end

    it "still generates intersection fields" do
      # Check for either naming convention
      mobile_us_present = field_names.include?("mobileUs") || field_names.include?("usMobile")
      desktop_us_present = field_names.include?("desktopUs") || field_names.include?("usDesktop")

      expect(mobile_us_present).to be(true)
      expect(desktop_us_present).to be(true)
    end
  end

  describe "#write_schemas" do
    let(:tables_directory) { Pathname.pwd.join("tables") }

    before do
      tables_directory.mkpath
      schema_manager.write_schemas(tables_directory)
    end

    it "generates a dimensions schema file" do
      expect(tables_directory.join("dimensions.json")).to be_file
    end

    it "generates a manifold schema file" do
      expect(tables_directory.join("manifold.json")).to be_file
    end

    it "generates a metrics directory" do
      expect(tables_directory.join("metrics")).to be_directory
    end

    it "generates a metrics schema file for each metrics group" do
      expect(tables_directory.join("metrics/renders.json")).to be_file
    end
  end
end
