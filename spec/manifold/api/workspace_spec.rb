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

      let(:vector_service) { instance_double(Manifold::Services::VectorService) }

      before do
        # Use mock for VectorService
        allow(Manifold::Services::VectorService).to receive(:new).and_return(vector_service)

        # Mock successful vector schema loading
        allow(vector_service).to receive(:load_vector_schema).with("User").and_return(
          {
            "name" => "user",
            "type" => "RECORD",
            "fields" => [
              { "name" => "user_id", "type" => "STRING", "mode" => "NULLABLE" },
              { "name" => "email", "type" => "STRING", "mode" => "NULLABLE" }
            ]
          }
        )

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
          timestamp:
            field: created_at
            interval: DAY
          metrics:
            taps:
              conditions:
                paid: IS_PAID(context.location)
                organic: IS_ORGANIC(context.location)
                us: context.geo.country = 'US'
                global: context.geo.country != 'US'
                retargeting: context.campaign_type = 'RETARGETING'
                prospecting: context.campaign_type = 'PROSPECTING'

              breakouts:
                acquisition:
                  - paid
                  - organic
                geography:
                  - us
                  - global
                campaign:
                  - retargeting
                  - prospecting

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

      it "includes user_id dimension field" do
        user_fields = fetch_user_dimension_fields
        expect(user_fields).to include(
          { "name" => "user_id", "type" => "STRING", "mode" => "NULLABLE" }
        )
      end

      it "includes email dimension field" do
        user_fields = fetch_user_dimension_fields
        expect(user_fields).to include(
          { "name" => "email", "type" => "STRING", "mode" => "NULLABLE" }
        )
      end

      def fetch_user_dimension_fields
        schema = parse_dimensions_schema
        dimensions = schema.find { |f| f["name"] == "dimensions" }
        user_dimension = dimensions["fields"].find { |f| f["name"] == "user" }
        user_dimension["fields"]
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

      it "generates a metrics table schema file for each metrics group" do
        expect(workspace.tables_directory.join("metrics/taps.json")).to be_file
      end

      it "includes required id field in metrics table schema" do
        metrics_schema = parse_metrics_schema("taps")
        expect(metrics_schema).to include(
          { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" }
        )
      end

      it "includes required timestamp field in metrics table schema" do
        metrics_schema = parse_metrics_schema("taps")
        expect(metrics_schema).to include(
          { "type" => "TIMESTAMP", "name" => "timestamp", "mode" => "REQUIRED" }
        )
      end

      it "includes metrics field of type RECORD in metrics table schema" do
        metrics_schema = parse_metrics_schema("taps")
        metrics_field = metrics_schema.find { |f| f["name"] == "metrics" }
        expect(metrics_field["type"]).to eq("RECORD")
      end

      it "includes metrics field with REQUIRED mode in metrics table schema" do
        metrics_schema = parse_metrics_schema("taps")
        metrics_field = metrics_schema.find { |f| f["name"] == "metrics" }
        expect(metrics_field["mode"]).to eq("REQUIRED")
      end

      it "includes metrics group with correct name in metrics table schema" do
        metrics_schema = parse_metrics_schema("taps")
        metrics_field = metrics_schema.find { |f| f["name"] == "metrics" }
        group_field = metrics_field["fields"].first
        expect(group_field["name"]).to eq("taps")
      end

      it "includes metrics group with RECORD type in metrics table schema" do
        metrics_schema = parse_metrics_schema("taps")
        metrics_field = metrics_schema.find { |f| f["name"] == "metrics" }
        group_field = metrics_field["fields"].first
        expect(group_field["type"]).to eq("RECORD")
      end

      shared_examples "breakout metrics" do |breakout_name|
        it "includes tapCount metric" do
          breakout = find_breakout(breakout_name)
          expect(breakout["fields"]).to include(
            { "type" => "INTEGER", "name" => "tapCount", "mode" => "NULLABLE" }
          )
        end

        it "includes sequenceSum metric" do
          breakout = find_breakout(breakout_name)
          expect(breakout["fields"]).to include(
            { "type" => "INTEGER", "name" => "sequenceSum", "mode" => "NULLABLE" }
          )
        end

        def find_breakout(name)
          schema_fields[:metrics]["fields"]
            .find { |f| f["name"] == "taps" }["fields"]
            .find { |f| f["name"] == name }
        end
      end

      include_examples "breakout metrics", "paid"
      include_examples "breakout metrics", "organic"
      include_examples "breakout metrics", "us"
      include_examples "breakout metrics", "global"
      include_examples "breakout metrics", "retargeting"
      include_examples "breakout metrics", "prospecting"

      # Test two-way intersection fields
      include_examples "breakout metrics", "paidUs"
      include_examples "breakout metrics", "paidGlobal"
      include_examples "breakout metrics", "organicUs"
      include_examples "breakout metrics", "organicGlobal"
      include_examples "breakout metrics", "paidRetargeting"
      include_examples "breakout metrics", "paidProspecting"
      include_examples "breakout metrics", "organicRetargeting"
      include_examples "breakout metrics", "organicProspecting"
      include_examples "breakout metrics", "usRetargeting"
      include_examples "breakout metrics", "usProspecting"
      include_examples "breakout metrics", "globalRetargeting"
      include_examples "breakout metrics", "globalProspecting"

      # Test three-way intersection fields
      include_examples "breakout metrics", "paidUsRetargeting"
      include_examples "breakout metrics", "paidUsProspecting"
      include_examples "breakout metrics", "paidGlobalRetargeting"
      include_examples "breakout metrics", "paidGlobalProspecting"
      include_examples "breakout metrics", "organicUsRetargeting"
      include_examples "breakout metrics", "organicUsProspecting"
      include_examples "breakout metrics", "organicGlobalRetargeting"
      include_examples "breakout metrics", "organicGlobalProspecting"

      it "includes all condition fields and intersection fields in the metrics fields" do
        expect(schema_fields[:metrics]["fields"]
          .find { |f| f["name"] == "taps" }["fields"]
          .map { |f| f["name"] })
          .to match_array(expected_field_names)
      end

      def expected_field_names
        %w[
          paid organic us global retargeting prospecting
          paidUs paidGlobal organicUs organicGlobal
          paidRetargeting paidProspecting organicRetargeting organicProspecting
          usRetargeting usProspecting globalRetargeting globalProspecting
          paidUsRetargeting paidUsProspecting paidGlobalRetargeting paidGlobalProspecting
          organicUsRetargeting organicUsProspecting organicGlobalRetargeting organicGlobalProspecting
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

      def parse_metrics_schema(group_name)
        JSON.parse(workspace.tables_directory.join("metrics/#{group_name}.json").read)
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
          metrics:
            taps:
              source: analytics.events
              conditions:
                paid: IS_PAID(context.location)
              breakouts:
                acquisition:
                  - paid
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
