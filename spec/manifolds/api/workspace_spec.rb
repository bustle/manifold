# frozen_string_literal: true

RSpec.describe Manifolds::API::Workspace do
  include FakeFS::SpecHelpers

  subject(:workspace) { described_class.new(name, project: project) }

  let(:project) { Manifolds::API::Project.new("wetland") }
  let(:name) { "people" }

  before do
    # Set up any template files that need to exist
    FileUtils.mkdir_p("#{File.dirname(__FILE__)}/../../../lib/manifolds/templates")
    File.write("#{File.dirname(__FILE__)}/../../../lib/manifolds/templates/workspace_template.yml",
               "vectors:\nmetrics:")
    File.write("#{File.dirname(__FILE__)}/../../../lib/manifolds/templates/vector_template.yml", "attributes:")
  end

  it { is_expected.to have_attributes(name: name, project: project) }

  describe ".add" do
    before { workspace.add }

    it { expect(workspace.routines_directory).to be_directory }
    it { expect(workspace.tables_directory).to be_directory }
    it { expect(File).to exist(workspace.manifold_path) }
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
end
