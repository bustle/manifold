# frozen_string_literal: true

require "fakefs/spec_helpers"

RSpec.describe Manifolds::Services::BigQueryService do
  include FakeFS::SpecHelpers

  let(:logger) { Logger.new(File::NULL) }
  let(:service) { described_class.new(logger) }
  let(:project_name) { "test_project" }

  before do
    FakeFS.activate!
    FileUtils.mkdir_p("./projects/#{project_name}")
  end

  after do
    FakeFS.deactivate!
  end

  describe "#generate_dimensions_schema" do
    context "when the project configuration exists" do
      before do
        # Create a test configuration
        FileUtils.mkdir_p("./vectors")
        File.write("./vectors/user.yml", <<~YAML)
          attributes:
            user_id: string
            email: string
        YAML

        File.write("./projects/#{project_name}/manifold.yml", <<~YAML)
          vectors:
            - User
        YAML

        service.generate_dimensions_schema(project_name)
      end

      it "generates a dimensions schema file" do
        schema_file = "./projects/#{project_name}/bq/tables/dimensions.json"
        expect(File.exist?(schema_file)).to be true
      end

      it "includes the expected schema structure" do
        schema = JSON.parse(File.read("./projects/#{project_name}/bq/tables/dimensions.json"))
        expect(schema).to include(
          {
            "type" => "STRING",
            "name" => "id",
            "mode" => "REQUIRED"
          }
        )
      end
    end

    context "when the project configuration is missing" do
      it "indicates the configuration is missing" do
        expect(logger).to receive(:error).with(/Config file missing for project/)
        service.generate_dimensions_schema(project_name)
      end
    end
  end
end
