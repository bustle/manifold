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
        YAML

        workspace.generate
      end

      it "generates a dimensions schema file" do
        expect(workspace.tables_directory.join("dimensions.json")).to be_file
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
        Pathname.pwd.join("lib/routines").mkpath
        Pathname.pwd.join("lib/routines/select_pages.sql")
                .write("SELECT id, STRUCT(url, title) AS dimensions FROM pages")

        workspace.add
        workspace.manifold_path.write(<<~YAML)
          vectors:
            - Page
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
