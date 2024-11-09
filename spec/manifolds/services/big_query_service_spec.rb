# frozen_string_literal: true

require "fakefs/spec_helpers"

RSpec.describe Manifolds::Services::BigQueryService do
  include FakeFS::SpecHelpers

  let(:logger) { instance_spy(Logger) }
  let(:service) { described_class.new(logger) }
  let(:project_name) { "test_project" }
  let(:dimensions_file) do
    Pathname.pwd.join("projects", project_name, "bq", "tables", "dimensions.json")
  end

  before do
    Pathname.pwd.join("projects", project_name).mkpath
  end

  describe "#generate_dimensions_schema" do
    context "when the project configuration exists" do
      before do
        # Create a test configuration
        FileUtils.mkdir_p(File.join(Dir.pwd, "vectors"))
        File.write(File.join(Dir.pwd, "vectors", "user.yml"), <<~YAML)
          attributes:
            user_id: string
            email: string
        YAML

        File.write(File.join(Dir.pwd, "projects", project_name, "manifold.yml"), <<~YAML)
          vectors:
            - User
        YAML

        service.generate_dimensions_schema(project_name)
      end

      it "generates a dimensions schema file" do
        expect(File.exist?(dimensions_file)).to be true
      end

      it "includes the expected schema structure" do
        schema = JSON.parse(File.read(dimensions_file))
        expect(schema).to include({ "type" => "STRING", "name" => "id", "mode" => "REQUIRED" })
      end
    end

    context "when the project configuration is missing" do
      it "indicates the configuration is missing" do
        service.generate_dimensions_schema(project_name)
        expect(logger).to have_received(:error)
          .with(/Config file missing for project/)
      end
    end
  end
end
