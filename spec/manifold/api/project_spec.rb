# frozen_string_literal: true

RSpec.describe Manifold::API::Project do
  include FakeFS::SpecHelpers

  subject(:project) { described_class.new(name) }

  let(:name) { "wetland" }

  include_context "with template files"

  it { is_expected.to have_attributes(name:) }

  describe ".create" do
    before { described_class.create(name) }

    it "creates the vectors directory" do
      expect(project.vectors_directory).to be_directory
    end

    it "creates the workspaces directory" do
      expect(project.workspaces_directory).to be_directory
    end
  end

  describe ".workspaces_directory" do
    it { expect(project.workspaces_directory).to be_an_instance_of(Pathname) }
  end

  describe ".vectors_directory" do
    it { expect(project.vectors_directory).to be_an_instance_of(Pathname) }
  end

  context "with directory" do
    subject(:project) { described_class.new(name, directory:) }

    let(:directory) { Pathname.pwd.join("supplied_directory") }

    it { is_expected.to have_attributes(directory:) }

    it "uses it as the base for the vectors directory" do
      expect(project.vectors_directory).to eq directory.join("vectors")
    end

    it "uses it as the base for the workspaces directory" do
      expect(project.workspaces_directory).to eq directory.join("workspaces")
    end
  end

  describe "#generate" do
    let(:workspace_one) { instance_double(Manifold::API::Workspace, name: "workspace_one") }
    let(:workspace_two) { instance_double(Manifold::API::Workspace, name: "workspace_two") }

    before do
      described_class.create(name)

      [workspace_one, workspace_two].each do |workspace|
        project.workspaces << workspace
        allow(workspace).to receive(:generate)
      end
    end

    it "calls generate on each workspace" do
      project.generate
      expect([workspace_one, workspace_two]).to all(have_received(:generate))
    end

    it "creates a terraform configuration file" do
      project.generate
      expect(project.directory.join("main.tf.json")).to be_file
    end

    it "includes workspace modules in the terraform configuration" do
      project.generate
      config = parse_terraform_config(project)
      expect(config["module"]).to include(expected_workspace_modules)
    end

    def parse_terraform_config(project)
      JSON.parse(project.directory.join("main.tf.json").read)
    end

    def expected_workspace_modules
      {
        "workspace_one" => { "source" => "./workspaces/workspace_one", "project_id" => "${var.PROJECT_ID}" },
        "workspace_two" => { "source" => "./workspaces/workspace_two", "project_id" => "${var.PROJECT_ID}" }
      }
    end
  end
end
