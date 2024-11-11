# frozen_string_literal: true

RSpec.describe Manifold::API::Project do
  include FakeFS::SpecHelpers

  subject(:project) { described_class.new(name) }

  let(:name) { "wetland" }

  it { is_expected.to have_attributes(name: name) }

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
    subject(:project) { described_class.new(name, directory: directory) }

    let(:directory) { Pathname.pwd.join("supplied_directory") }

    it { is_expected.to have_attributes(directory: directory) }

    it "uses it as the base for the vectors directory" do
      expect(project.vectors_directory).to eq directory.join("vectors")
    end

    it "uses it as the base for the workspaces directory" do
      expect(project.workspaces_directory).to eq directory.join("workspaces")
    end
  end
end
