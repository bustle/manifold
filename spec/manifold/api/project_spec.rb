# frozen_string_literal: true

RSpec.describe Manifold::API::Project do
  include FakeFS::SpecHelpers

  subject(:project) { described_class.new(name) }

  let(:name) { "my_project" }

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

    context "with terraform disabled" do
      it "does not generate terraform configurations" do
        project.generate(with_terraform: false)
        expect(project.directory.join("main.tf.json")).not_to be_file
      end
    end

    context "with terraform enabled" do
      it "creates a terraform configuration file" do
        project.generate(with_terraform: true)
        expect(project.directory.join("main.tf.json")).to be_file
      end

      it "includes workspace modules in the configuration" do
        project.generate(with_terraform: true)
        config = parse_terraform_config(project)
        expect(config["module"]).to include(expected_workspace_modules)
      end
    end

    context "with terraform submodule" do
      it "excludes provider configuration" do
        project.generate(with_terraform: true, is_submodule: true)
        config = parse_terraform_config(project)
        expect(config["terraform"]).to be_nil
      end

      it "excludes provider block" do
        project.generate(with_terraform: true, is_submodule: true)
        config = parse_terraform_config(project)
        expect(config["provider"]).to be_nil
      end

      it "includes workspace modules" do
        project.generate(with_terraform: true, is_submodule: true)
        config = parse_terraform_config(project)
        expect(config["module"]).to include(expected_workspace_modules)
      end
    end

    def parse_terraform_config(project)
      JSON.parse(project.directory.join("main.tf.json").read)
    end

    def expected_workspace_modules
      {
        "workspace_one" => { "source" => "./workspaces/workspace_one", "project_id" => "${var.project_id}" },
        "workspace_two" => { "source" => "./workspaces/workspace_two", "project_id" => "${var.project_id}" }
      }
    end
  end
end
