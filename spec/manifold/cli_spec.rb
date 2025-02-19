# frozen_string_literal: true

RSpec.describe Manifold::CLI do
  include FakeFS::SpecHelpers

  let(:null_logger) { instance_double(Logger) }
  let(:mock_project) { instance_double(Manifold::API::Project) }
  let(:mock_workspace) { instance_double(Manifold::API::Workspace) }
  let(:mock_vector) { instance_double(Manifold::API::Vector) }

  before do
    allow(Manifold::API::Project).to receive(:new).and_return(mock_project)
    allow(Manifold::API::Workspace).to receive(:new).and_return(mock_workspace)
    allow(Manifold::API::Vector).to receive(:new).and_return(mock_vector)
    allow(null_logger).to receive(:info)
    allow(null_logger).to receive(:level=)
  end

  describe "#init" do
    subject(:cli) { described_class.new(logger: null_logger) }

    let(:project_name) { "my_project" }

    context "when initializing a new project" do
      before do
        allow(Manifold::API::Project).to receive(:create).and_return(mock_project)
      end

      it "creates a new project through the API" do
        cli.init(project_name)
        expect(Manifold::API::Project).to have_received(:create).with(project_name)
      end

      it "logs the project creation" do
        cli.init(project_name)
        expect(null_logger).to have_received(:info)
          .with("Created umbrella project '#{project_name}' with workspaces and vectors directories.")
      end
    end
  end

  describe "#generate" do
    subject(:cli) { described_class.new(logger: null_logger) }

    before do
      allow(mock_project).to receive(:generate)
    end

    context "with default options" do
      before do
        cli.options = { submodule: true }
      end

      it "generates terraform configurations as a submodule" do
        cli.generate
        expect(mock_project).to have_received(:generate)
          .with(with_terraform: true, is_submodule: true)
      end
    end

    context "with --no-submodule option" do
      before do
        cli.options = { submodule: false }
      end

      it "generates terraform configurations with provider" do
        cli.generate
        expect(mock_project).to have_received(:generate)
          .with(with_terraform: true, is_submodule: false)
      end
    end

    it "logs the generation" do
      cli.generate
      expect(null_logger).to have_received(:info)
        .with("Generated BigQuery schema and Terraform configurations for all workspaces in the project.")
    end
  end

  describe "#add" do
    subject(:cli) { described_class.new(logger: null_logger) }

    let(:workspace_name) { "Commerce" }

    context "when adding a workspace" do
      before do
        allow(mock_workspace).to receive(:add)
        cli.add(workspace_name)
      end

      it "instantiates a new workspace through the API" do
        expect(Manifold::API::Workspace).to have_received(:new).with(workspace_name)
      end

      it "adds the workspace through the API" do
        expect(mock_workspace).to have_received(:add)
      end

      it "logs the workspace creation" do
        expect(null_logger).to have_received(:info)
          .with("Added workspace '#{workspace_name}' with tables and routines directories.")
      end
    end
  end

  describe "vectors#add" do
    subject(:cli) do
      subcommands = described_class.new.class.subcommand_classes
      subcommands["vectors"].new(logger: null_logger)
    end

    let(:vector_name) { "page" }

    context "when adding a vector" do
      before do
        allow(mock_vector).to receive(:add)
        cli.add(vector_name)
      end

      it "instantiates a new vector through the API" do
        expect(Manifold::API::Vector).to have_received(:new).with(vector_name)
      end

      it "adds the vector through the API" do
        expect(mock_vector).to have_received(:add)
      end

      it "logs the vector creation" do
        expect(null_logger).to have_received(:info)
          .with("Created vector configuration for '#{vector_name}'.")
      end
    end
  end
end
