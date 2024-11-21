# frozen_string_literal: true

RSpec.describe Manifold::API::Workspace do
  include FakeFS::SpecHelpers

  subject(:workspace) { described_class.new(name, logger: logger) }

  let(:logger) { instance_spy(Logger) }
  let(:name) { "people" }

  include_context "with template files"

  it { is_expected.to have_attributes(name: name) }

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

      it "includes the expected schema structure" do
        schema = JSON.parse(workspace.tables_directory.join("dimensions.json").read)
        expect(schema).to include(
          { "type" => "STRING", "name" => "id", "mode" => "REQUIRED" }
        )
      end

      it "logs vector schema loading" do
        expect(logger).to have_received(:info).with("Loading vector schema for 'User'.")
      end

      it "logs successful generation" do
        expect(logger).to have_received(:info)
          .with("Generated BigQuery dimensions table schema for workspace '#{name}'.")
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
  end
end
