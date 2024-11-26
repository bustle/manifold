# frozen_string_literal: true

RSpec.describe Manifold::Terraform::ProjectConfiguration do
  include FakeFS::SpecHelpers

  subject(:config) { described_class.new(workspaces) }

  let(:workspaces) do
    [
      instance_double(Manifold::API::Workspace, name: "workspace_one"),
      instance_double(Manifold::API::Workspace, name: "workspace_two")
    ]
  end

  describe "#as_json" do
    subject(:json) { config.as_json }

    it "includes required provider configuration" do
      expect(json["terraform"]["required_providers"]).to include(expected_google_provider)
    end

    it "includes provider configuration" do
      expect(json["provider"]).to include(
        "google" => {
          "project" => "${var.project_id}"
        }
      )
    end

    it "includes project_id variable" do
      expect(json["variable"]["project_id"]).to include(
        "description" => "The GCP project ID where resources will be created",
        "type" => "string"
      )
    end

    it "writes workspace modules with correct project_id and source" do
      expect(json["module"]).to include(
        "workspace_one" => { "project_id" => "${var.project_id}", "source" => "./workspaces/workspace_one" },
        "workspace_two" => { "project_id" => "${var.project_id}", "source" => "./workspaces/workspace_two" }
      )
    end

    def expected_google_provider
      {
        "google" => {
          "source" => "hashicorp/google",
          "version" => "~> 4.0"
        }
      }
    end
  end
end
