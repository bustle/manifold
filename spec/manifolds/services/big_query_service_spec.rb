# frozen_string_literal: true

require "fakefs/spec_helpers"

RSpec.describe Manifolds::Services::BigQueryService do
  include FakeFS::SpecHelpers

  let(:logger) { instance_spy(Logger) }
  let(:service) { described_class.new(logger) }
  let(:project_name) { "test_project" }
  let(:dimensions_path) do
    Pathname.pwd.join("projects", project_name, "bq", "tables", "dimensions.json")
  end

  before do
    Pathname.pwd.join("projects", project_name).mkpath
  end

  describe "#generate_dimensions_schema" do
    context "when the project configuration exists" do
      before do
        Pathname.pwd.join("vectors").mkpath
        Pathname.pwd.join("vectors", "user.yml").write(<<~YAML)
          attributes:
            user_id: string
            email: string
        YAML

        Pathname.pwd.join("projects", project_name, "manifold.yml").write(<<~YAML)
          vectors:
            - User
        YAML

        service.generate_dimensions_schema(project_name)
      end

      it "generates a dimensions schema file" do
        expect(dimensions_path.file?).to be true
      end

      it "includes the expected schema structure" do
        schema = JSON.parse(dimensions_path.read)
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
