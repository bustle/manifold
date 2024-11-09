# frozen_string_literal: true

RSpec.describe Manifolds::API::Vector do
  include FakeFS::SpecHelpers

  subject(:vector) { described_class.new(name, project: project) }

  let(:project) { Manifolds::API::Project.new("wetland") }
  let(:name) { "page" }

  before do
    # Set up any template files that need to exist
    FileUtils.mkdir_p("#{File.dirname(__FILE__)}/../../../lib/manifolds/templates")
    File.write("#{File.dirname(__FILE__)}/../../../lib/manifolds/templates/workspace_template.yml",
               "vectors:\nmetrics:")
    File.write("#{File.dirname(__FILE__)}/../../../lib/manifolds/templates/vector_template.yml", "attributes:")
  end

  it { is_expected.to have_attributes(name: name, project: project) }

  describe ".add" do
    before { vector.add }

    it { expect(vector.routines_directory).to be_directory }
    it { expect(vector.tables_directory).to be_directory }
    it { expect(File).to exist(vector.config_template_path) }
  end

  describe ".routines_directory" do
    it { expect(vector.routines_directory).to be_an_instance_of(Pathname) }
  end

  describe ".tables_directory" do
    it { expect(vector.tables_directory).to be_an_instance_of(Pathname) }
  end

  describe ".config_template_path" do
    it { expect(vector.config_template_path).to be_an_instance_of(Pathname) }
  end
end
