# frozen_string_literal: true

RSpec.describe Manifolds::CLI do
  include FakeFS::SpecHelpers

  let(:project_name) { "wetland" }
  let(:workspace_name) { "Commerce" }
  let(:null_logger) { instance_double(Logger) }
  let(:mock_project) { instance_double(Manifolds::API::Project) }
  let(:mock_workspace) { instance_double(Manifolds::API::Workspace) }
  let(:mock_vector) { instance_double(Manifolds::API::Vector) }

  before do
    allow(Manifolds::API::Project).to receive(:new).and_return(mock_project)
    allow(Manifolds::API::Workspace).to receive(:new).and_return(mock_workspace)
    allow(Manifolds::API::Vector).to receive(:new).and_return(mock_vector)
    allow(null_logger).to receive(:info)
    allow(null_logger).to receive(:level=)
  end

  describe "#init" do
    subject(:cli) { described_class.new(logger: null_logger) }

    context "when initializing a new project" do
      before do
        allow(mock_project).to receive(:init)
        cli.init(project_name)
      end

      it "instantiates a new project through the API" do
        expect(Manifolds::API::Project).to have_received(:new).with(project_name)
      end

      it "initializes the project through the API" do
        expect(mock_project).to have_received(:init)
      end

      it "logs the project creation" do
        expect(null_logger).to have_received(:info)
          .with("Created umbrella project '#{project_name}' with projects and vectors directories.")
      end
    end
  end

  describe "#add" do
    subject(:cli) { described_class.new(logger: null_logger) }

    context "when adding a workspace" do
      before do
        allow(mock_workspace).to receive(:add)
        cli.add(workspace_name)
      end

      it "instantiates a new workspace through the API" do
        expect(Manifolds::API::Workspace).to have_received(:new)
          .with(workspace_name, project: mock_project)
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
    subject(:cli) { vectors_command.new(logger: null_logger) }

    let(:vector_name) { "page" }
    let(:vectors_command) { described_class.new.class.subcommand_classes["vectors"] }

    context "when adding a vector" do
      before do
        allow(mock_vector).to receive(:add)
        cli.add(vector_name)
      end

      it "instantiates a new vector through the API" do
        expect(Manifolds::API::Vector).to have_received(:new)
          .with(vector_name, project: mock_project)
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
