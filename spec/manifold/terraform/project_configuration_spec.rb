# frozen_string_literal: true

RSpec.describe Manifold::Terraform::ProjectConfiguration do
  include FakeFS::SpecHelpers

  subject(:config) { described_class.new(workspaces, skip_provider_config:) }

  let(:workspaces) do
    [
      instance_double(Manifold::API::Workspace, name: "workspace_one"),
      instance_double(Manifold::API::Workspace, name: "workspace_two")
    ]
  end
  let(:skip_provider_config) { false }

  describe "#as_json" do
    subject(:json) { config.as_json }

    context "when skip_provider_config is false" do
      it "includes Google provider configuration" do
        expect(json["terraform"]["required_providers"]["google"]["source"]).to eq("hashicorp/google")
      end

      it "includes provider configuration" do
        expect(json["provider"]).to include(
          "google" => {
            "project" => "${var.project_id}"
          }
        )
      end
    end

    context "when skip_provider_config is true" do
      let(:skip_provider_config) { true }

      it "does not include terraform configuration" do
        expect(json["terraform"]).to be_nil
      end

      it "does not include provider configuration" do
        expect(json["provider"]).to be_nil
      end
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
  end
end
