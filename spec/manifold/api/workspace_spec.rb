# frozen_string_literal: true

RSpec.describe Manifold::API::Workspace do
  include FakeFS::SpecHelpers

  subject(:workspace) { described_class.new(name, logger:) }

  let(:logger) { instance_spy(Logger) }
  let(:name) { "people" }

  include_context "with template files"

  it { is_expected.to have_attributes(name:) }

  describe ".add" do
    before { workspace.add }

    it "creates the routines directory" do
      expect(workspace.routines_directory).to be_directory
    end

    it "creates the tables directory" do
      expect(workspace.tables_directory).to be_directory
    end

    it "creates the manifold file" do
      expect(File).to exist(workspace.manifold_path)
    end
  end

  describe ".routines_directory" do
    it { expect(workspace.routines_directory).to be_an_instance_of(Pathname) }
  end

  describe ".tables_directory" do
    it { expect(workspace.tables_directory).to be_an_instance_of(Pathname) }
  end

  context "when not created" do
    describe ".manifold_exists?" do
      it { expect(workspace.manifold_exists?).to be false }
    end

    describe ".manifold_file" do
      it { expect(workspace.manifold_file).to be_nil }
    end
  end

  context "when created" do
    before { workspace.add }

    describe ".manifold_exists?" do
      it { expect(workspace.manifold_exists?).to be true }
    end

    describe ".manifold_file" do
      it { expect(workspace.manifold_file).to be_an_instance_of(File) }
    end
  end

  describe "#generate" do
    context "when the manifold configuration exists" do
      let(:manifold_schema) { parse_manifold_schema }
      let(:schema_fields) do
        {
          metrics: manifold_schema.find { |f| f["name"] == "metrics" },
          basic: manifold_schema.map { |f| f.slice("type", "name", "mode") }
        }
      end

      before do
        Pathname.pwd.join("vectors").mkpath
        Pathname.pwd.join("vectors", "user.yml").write(<<~YAML)
          attributes:
            user_id: string
            email: string
        YAML

        workspace.add
        workspace.manifold_path.write(<<~YAML)
          vectors:
            - User
          breakouts:
            paid: IS_PAID(context.location)
            organic: IS_ORGANIC(context.location)
            paidOrganic:
              fields:
                - paid
                - organic
              operator: AND
            paidOrOrganic:
              fields:
                - paid
                - organic
              operator: OR
            notPaid:
              fields:
                - paid
              operator: NOT
            neitherPaidNorOrganic:
              fields:
                - paid
                - organic
              operator: NOR
            notBothPaidAndOrganic:
              fields:
                - paid
                - organic
              operator: NAND
            eitherPaidOrOrganic:
              fields:
                - paid
                - organic
              operator: XOR
            similarPaidOrganic:
              fields:
                - paid
                - organic
              operator: XNOR
          aggregations:
            countif: tapCount
            sumif:
              sequenceSum:
                field: context.sequence
        YAML

        workspace.generate
      end

      it "generates a dimensions schema file" do
        expect(workspace.tables_directory.join("dimensions.json")).to be_file
      end

      it "generates a manifold schema file" do
        expect(workspace.tables_directory.join("manifold.json")).to be_file
      end

      it "sets the ID field" do
        schema = parse_dimensions_schema
        expect(schema).to include({ "type" => "STRING", "name" => "id", "mode" => "REQUIRED" })
      end

      it "sets the dimensions fields" do
        expect(get_dimension("user")["fields"]).to include(
          { "type" => "STRING", "name" => "user_id", "mode" => "NULLABLE" },
          { "type" => "STRING", "name" => "email", "mode" => "NULLABLE" }
        )
      end

      it "includes required id field in manifold schema" do
        expect(schema_fields[:basic]).to include(
          { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" }
        )
      end

      it "includes required timestamp field in manifold schema" do
        expect(schema_fields[:basic]).to include(
          { "type" => "TIMESTAMP", "name" => "timestamp", "mode" => "REQUIRED" }
        )
      end

      it "includes required dimensions field in manifold schema" do
        expect(schema_fields[:basic]).to include(
          { "type" => "RECORD", "name" => "dimensions", "mode" => "REQUIRED" }
        )
      end

      it "sets the metrics type to RECORD" do
        expect(schema_fields[:metrics]["type"]).to eq("RECORD")
      end

      it "sets the metrics mode to REQUIRED" do
        expect(schema_fields[:metrics]["mode"]).to eq("REQUIRED")
      end

      shared_examples "context metrics" do |breakout_name|
        let(:context) { schema_fields[:metrics]["fields"].find { |f| f["name"] == breakout_name } }

        it "includes tapCount metric" do
          expect(context["fields"]).to include(
            { "type" => "INTEGER", "name" => "tapCount", "mode" => "NULLABLE" }
          )
        end

        it "includes sequenceSum metric" do
          expect(context["fields"]).to include(
            { "type" => "INTEGER", "name" => "sequenceSum", "mode" => "NULLABLE" }
          )
        end
      end

      include_examples "context metrics", "paid"
      include_examples "context metrics", "organic"
      include_examples "context metrics", "paidOrganic"
      include_examples "context metrics", "paidOrOrganic"
      include_examples "context metrics", "notPaid"
      include_examples "context metrics", "neitherPaidNorOrganic"
      include_examples "context metrics", "notBothPaidAndOrganic"
      include_examples "context metrics", "eitherPaidOrOrganic"
      include_examples "context metrics", "similarPaidOrganic"

      it "includes all breakouts in the metrics fields" do
        expect(schema_fields[:metrics]["fields"].map { |f| f["name"] })
          .to match_array(expected_breakout_names)
      end

      def expected_breakout_names
        %w[
          paid organic paidOrganic paidOrOrganic notPaid
          neitherPaidNorOrganic notBothPaidAndOrganic
          eitherPaidOrOrganic similarPaidOrganic
        ]
      end

      it "logs vector schema loading" do
        expect(logger).to have_received(:info).with("Loading vector schema for 'User'.")
      end

      it "logs successful generation" do
        expect(logger).to have_received(:info)
          .with("Generated BigQuery dimensions table schema for workspace '#{name}'.")
      end

      def parse_dimensions_schema
        JSON.parse(workspace.tables_directory.join("dimensions.json").read)
      end

      def parse_manifold_schema
        JSON.parse(workspace.tables_directory.join("manifold.json").read)
      end

      def get_dimension(field)
        dimensions = parse_dimensions_schema.find { |f| f["name"] == "dimensions" }
        dimensions["fields"].find { |f| f["name"] == field }
      end
    end

    context "when the manifold configuration is missing" do
      it "returns nil" do
        expect(workspace.generate).to be_nil
      end
    end

    context "when the manifold configuration has no vectors" do
      before do
        workspace.add
        workspace.manifold_path.write("vectors:\n")
        workspace.generate
      end

      it "returns nil" do
        expect(workspace.generate).to be_nil
      end
    end

    context "when generating with terraform" do
      let(:vector_service) { instance_double(Manifold::Services::VectorService) }

      before do
        configure_vector_service
        setup_workspace_files
        workspace.generate(with_terraform: true)
      end

      it "generates terraform configuration" do
        expect(workspace.terraform_main_path).to be_file
      end

      it "loads vector configurations" do
        expect(vector_service).to have_received(:load_vector_config).with("Page")
      end

      it "includes vector configurations in terraform" do
        config = JSON.parse(workspace.terraform_main_path.read)
        expect(config["resource"]["google_bigquery_routine"]).not_to be_nil
      end

      it "generates the manifold merge SQL file" do
        expect(workspace.routines_directory.join("merge_manifold.sql")).to be_file
      end

      it "includes the merge SQL in the generated file" do
        sql = workspace.routines_directory.join("merge_manifold.sql").read
        expect(sql).to include("MERGE #{workspace.name}.Manifold AS target")
      end

      def configure_vector_service
        allow(Manifold::Services::VectorService).to receive(:new).and_return(vector_service)
        configure_vector_schema
        configure_vector_config
      end

      def configure_vector_schema
        allow(vector_service).to receive(:load_vector_schema)
          .with("Page")
          .and_return(vector_schema)
      end

      def configure_vector_config
        allow(vector_service).to receive(:load_vector_config)
          .with("Page")
          .and_return(vector_config)
      end

      def setup_workspace_files
        setup_routines_directory
        setup_manifold_config
      end

      def setup_routines_directory
        Pathname.pwd.join("lib/routines").mkpath
        Pathname.pwd.join("lib/routines/select_pages.sql")
                .write("SELECT id, STRUCT(url, title) AS dimensions FROM pages")
      end

      def setup_manifold_config
        workspace.add
        workspace.manifold_path.write(manifold_yaml_content)
      end

      def manifold_yaml_content
        <<~YAML
          vectors:
            - Page
          dimensions:
            merge:
              source: lib/routines/select_pages.sql
          source: analytics.events
          timestamp:
            field: created_at
            interval: DAY
          breakouts:
            paid: IS_PAID(context.location)
          aggregations:
            countif: tapCount
        YAML
      end

      def vector_schema
        {
          "name" => "page",
          "type" => "RECORD",
          "fields" => [
            { "name" => "url", "type" => "STRING", "mode" => "NULLABLE" },
            { "name" => "title", "type" => "STRING", "mode" => "NULLABLE" }
          ]
        }
      end

      def vector_config
        {
          "name" => "page",
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
  end
end
