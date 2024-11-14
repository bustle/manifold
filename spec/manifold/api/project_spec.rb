# frozen_string_literal: true

RSpec.describe(Manifold::API::Project) do
  include FakeFS::SpecHelpers

  subject(:project) { described_class.new(config: config) }

  let(:config) { Pathname.pwd.join("project.yaml") }

  it { is_expected.to have_attributes(config_path: config) }

  describe ".directory" do
    it { expect(project.directory).to be_an_instance_of(Pathname) }
  end

  describe ".workspaces_directory" do
    it { expect(project.workspaces_directory).to be_an_instance_of(Pathname) }
  end

  describe ".vectors_directory" do
    it { expect(project.vectors_directory).to be_an_instance_of(Pathname) }
  end

  context "when not created" do
    describe ".created?" do
      it { expect(project.created?).to be false }
    end

    describe ".config" do
      it { expect(project.config).to be nil }
    end

    describe ".create" do
      before { project.create }

      it { expect(project.config_path).to be_file }
      it { expect(project.vectors_directory).to be_directory }
      it { expect(project.workspaces_directory).to be_directory }
    end
  end

  context "when created" do
    before { project.create }

    describe ".created?" do
      it { expect(project.created?).to be true }
    end

    describe ".config" do
      it { expect(project.config).to be_a(Hash) }
    end
  end
end
