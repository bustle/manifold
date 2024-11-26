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

    it "includes Google provider configuration" do
      expect(json["terraform"]["required_providers"]["google"]["source"]).to eq("hashicorp/google")
    end

    it "includes provider configuration" do
      expect(json["provider"]).to include(
        "google" => {
          "project" => "${var.PROJECT_ID}"
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
        "workspace_one" => { "project_id" => "${var.PROJECT_ID}", "source" => "./workspaces/workspace_one" },
        "workspace_two" => { "project_id" => "${var.PROJECT_ID}", "source" => "./workspaces/workspace_two" }
      )
    end
  end
end
