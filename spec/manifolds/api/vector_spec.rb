# frozen_string_literal: true

RSpec.describe Manifold::API::Vector do
  include FakeFS::SpecHelpers
  subject(:vector) { described_class.new(name, project: project) }

  include_context "with template files"

  let(:project) { Manifold::API::Project.new("wetland") }
  let(:name) { "page" }

  it { is_expected.to have_attributes(name: name, project: project) }

  describe ".add" do
    before { vector.add }

    it "creates the config template file" do
      expect(File).to exist(vector.template_path)
    end
  end

  describe ".template_path" do
    it { expect(vector.template_path).to be_an_instance_of(Pathname) }
  end
end
