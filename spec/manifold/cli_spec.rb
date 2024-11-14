# frozen_string_literal: true

RSpec.describe Manifold::CLI do
  include FakeFS::SpecHelpers
  subject(:cli) { described_class.new(logger: logger, project: project) }

  let(:project) { Manifold::API::Project.new }
  let(:logger) { Logger.new(IO::NULL) }

  # let(:null_logger) { instance_double(Logger) }
  # let(:mock_project) { instance_double(Manifold::API::Project) }
  # let(:mock_workspace) { instance_double(Manifold::API::Workspace) }
  # let(:mock_vector) { instance_double(Manifold::API::Vector) }

  # before do
  #   allow(Manifold::API::Project).to receive(:new).and_return(mock_project)
  #   allow(Manifold::API::Workspace).to receive(:new).and_return(mock_workspace)
  #   allow(Manifold::API::Vector).to receive(:new).and_return(mock_vector)
  #   allow(null_logger).to receive(:info)
  #   allow(null_logger).to receive(:level=)
  # end

  describe "#init" do
    before do
      allow(project).to receive(:create)
      allow(logger).to receive(:info)
      cli.init
    end

    it { expect(project).to have_received(:create) }
    it { expect(logger).to have_received(:info) }
  end

  describe "#add" do
    let(:name) { "workspace_name" }
    let(:workspace) { Manifold::API::Workspace.new(name, project: project) }

    before do
      allow(workspace).to receive(:add)
      cli.add(name)
    end

    it { expect(workspace).to have_received(:add) }
    it { expect(logger).to have_received(:info) }
  end

  describe "vector subcommand" do
    let(:name) { "vector_name" }
    let(:vector) { Manifold::API::Vector.new(name, project: project) }

    describe "#add" do
      before do
        allow(vector).to receive(:add)
        cli.add(name)
      end

      it { expect(vector).to have_received(:add) }
      it { expect(logger).to have_received(:info) }
    end
  end
end
