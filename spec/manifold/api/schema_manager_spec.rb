# frozen_string_literal: true

RSpec.describe Manifold::API::SchemaManager do
  include FakeFS::SpecHelpers

  subject(:schema_manager) { described_class.new(name, vectors, vector_service, manifold_yaml, logger) }

  let(:logger) { instance_spy(Logger) }
  let(:name) { "test_workspace" }
  let(:vectors) { ["TestVector"] }
  let(:vector_service) { instance_spy(Manifold::Services::VectorService) }
  let(:manifold_yaml) { build_test_manifold_yaml }

  before do
    # Mock the vector service
    allow(vector_service).to receive(:load_vector_schema).and_return(
      { "name" => "test_vector", "type" => "STRING", "mode" => "NULLABLE" }
    )
  end

  # rubocop:disable Metrics/MethodLength
  def build_test_manifold_yaml
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
  # rubocop:enable Metrics/MethodLength

  describe "#dimensions_schema" do
    subject(:schema) { schema_manager.dimensions_schema }

    it "includes required id field" do
      expect(schema).to include(
        { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" }
      )
    end

    it "includes dimensions field with RECORD type" do
      dimensions_field = schema.find { |f| f["name"] == "dimensions" }
      expect(dimensions_field["type"]).to eq("RECORD")
    end

    it "includes dimensions field with REQUIRED mode" do
      dimensions_field = schema.find { |f| f["name"] == "dimensions" }
      expect(dimensions_field["mode"]).to eq("REQUIRED")
    end
  end

  describe "#manifold_schema" do
    subject(:schema) { schema_manager.manifold_schema }

    it "includes required id field" do
      expect(schema).to include(
        { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" }
      )
    end

    it "includes required timestamp field" do
      expect(schema).to include(
        { "type" => "TIMESTAMP", "name" => "timestamp", "mode" => "REQUIRED" }
      )
    end

    it "includes dimensions field with RECORD type" do
      dimensions_field = schema.find { |f| f["name"] == "dimensions" }
      expect(dimensions_field["type"]).to eq("RECORD")
    end

    it "includes metrics field with RECORD type" do
      metrics_field = schema.find { |f| f["name"] == "metrics" }
      expect(metrics_field["type"]).to eq("RECORD")
    end
  end

  describe "#metrics_fields" do
    # Using a simple helper method to clean up the spec and reduce memoized variables
    def render_fields
      metrics_fields = schema_manager.send(:metrics_fields)
      renders_field = metrics_fields.find { |field| field["name"] == "renders" }
      renders_field["fields"]
    end

    it "includes the renders group field" do
      metrics_fields = schema_manager.send(:metrics_fields)
      renders_field = metrics_fields.find { |field| field["name"] == "renders" }
      expect(renders_field).not_to be_nil
    end

    it "includes all individual condition fields" do
      field_names = render_fields.map { |field| field["name"] }
      expect(field_names).to include("mobile", "desktop", "us", "global")
    end

    describe "intersection fields" do
      it "includes mobile-us intersection" do
        field_names = render_fields.map { |field| field["name"] }
        expect(field_names.any? { |name| %w[mobileUs usMobile].include?(name) }).to be true
      end

      it "includes desktop-us intersection" do
        field_names = render_fields.map { |field| field["name"] }
        expect(field_names.any? { |name| %w[desktopUs usDesktop].include?(name) }).to be true
      end

      it "includes mobile-global intersection" do
        field_names = render_fields.map { |field| field["name"] }
        expect(field_names.any? { |name| %w[mobileGlobal globalMobile].include?(name) }).to be true
      end

      it "includes desktop-global intersection" do
        field_names = render_fields.map { |field| field["name"] }
        expect(field_names.any? { |name| %w[desktopGlobal globalDesktop].include?(name) }).to be true
      end
    end

    describe "exclusion of invalid intersections" do
      it "does not include mobile-desktop intersection" do
        field_names = render_fields.map { |field| field["name"] }
        expect(field_names).not_to include("mobileDesktop", "desktopMobile")
      end

      it "does not include us-global intersection" do
        field_names = render_fields.map { |field| field["name"] }
        expect(field_names).not_to include("usGlobal", "globalUs")
      end
    end

    describe "aggregation fields" do
      it "includes correct aggregation fields for individual conditions" do
        mobile_field = render_fields.find { |field| field["name"] == "mobile" }
        aggregation_names = mobile_field["fields"].map { |f| f["name"] }
        expect(aggregation_names).to include("renderCount", "sequenceSum")
      end

      it "includes renderCount in intersection fields" do
        intersection_field = find_intersection_field
        expect(intersection_field["fields"].map { |f| f["name"] }).to include("renderCount")
      end

      it "includes sequenceSum in intersection fields" do
        intersection_field = find_intersection_field
        expect(intersection_field["fields"].map { |f| f["name"] }).to include("sequenceSum")
      end

      def find_intersection_field
        render_fields.find { |field| field["name"] =~ /mobile.*us|us.*mobile/i }
      end
    end
  end

  describe "when conditions are not explicitly defined" do
    subject(:conditional_fields) { metric_render_fields }

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

    def metric_render_fields
      metrics_fields = schema_manager.send(:metrics_fields)
      renders_field = metrics_fields.find { |field| field["name"] == "renders" }
      renders_field["fields"]
    end

    it "derives condition fields from breakouts" do
      condition_names = conditional_fields.map { |field| field["name"] }
      expect(condition_names).to include("mobile", "desktop", "us", "global")
    end

    it "generates mobile-us intersection" do
      condition_names = conditional_fields.map { |field| field["name"] }
      mobile_us_variants = %w[mobileUs usMobile]
      expect(mobile_us_variants.any? { |variant| condition_names.include?(variant) }).to be true
    end

    it "generates desktop-us intersection" do
      condition_names = conditional_fields.map { |field| field["name"] }
      desktop_us_variants = %w[desktopUs usDesktop]
      expect(desktop_us_variants.any? { |variant| condition_names.include?(variant) }).to be true
    end
  end

  describe "#write_schemas" do
    subject(:tables_dir) { Pathname.pwd.join("tables") }

    before do
      tables_dir.mkpath
      schema_manager.write_schemas(tables_dir)
    end

    it "generates a dimensions schema file" do
      expect(tables_dir.join("dimensions.json")).to be_file
    end

    it "generates a manifold schema file" do
      expect(tables_dir.join("manifold.json")).to be_file
    end

    it "generates a metrics directory" do
      expect(tables_dir.join("metrics")).to be_directory
    end

    it "generates a metrics schema file for each metrics group" do
      expect(tables_dir.join("metrics/renders.json")).to be_file
    end
  end
end
