# frozen_string_literal: true

RSpec.describe Manifold::Services::VectorService do
  include FakeFS::SpecHelpers

  let(:logger) { instance_double(Logger) }
  let(:service) { described_class.new(logger) }

  describe "#load_vector_schema" do
    let(:vector_name) { "page" }
    let(:vector_config) do
      {
        "attributes" => {
          "id" => "string",
          "url" => "string",
          "created_at" => "timestamp",
          "tags" => "string:repeated",
          "email" => "string:required",
          "metadata" => {
            "source" => "string",
            "last_modified" => "timestamp",
            "settings" => {
              "enabled" => "boolean",
              "flags" => "string:repeated"
            }
          },
          "key_values" => {
            "first_key" => "string",
            "second_key" => "string:required"
          }
        }
      }
    end

    let(:expected_schema) do
      {
        "name" => "page",
        "type" => "RECORD",
        "fields" => [
          { "name" => "id", "type" => "STRING", "mode" => "NULLABLE" },
          { "name" => "url", "type" => "STRING", "mode" => "NULLABLE" },
          { "name" => "created_at", "type" => "TIMESTAMP", "mode" => "NULLABLE" },
          { "name" => "tags", "type" => "STRING", "mode" => "REPEATED" },
          { "name" => "email", "type" => "STRING", "mode" => "REQUIRED" },
          {
            "name" => "metadata",
            "type" => "RECORD",
            "mode" => "NULLABLE",
            "fields" => [
              { "name" => "source", "type" => "STRING", "mode" => "NULLABLE" },
              { "name" => "last_modified", "type" => "TIMESTAMP", "mode" => "NULLABLE" },
              {
                "name" => "settings",
                "type" => "RECORD",
                "mode" => "NULLABLE",
                "fields" => [
                  { "name" => "enabled", "type" => "BOOLEAN", "mode" => "NULLABLE" },
                  { "name" => "flags", "type" => "STRING", "mode" => "REPEATED" }
                ]
              }
            ]
          },
          {
            "name" => "key_values",
            "type" => "RECORD",
            "mode" => "NULLABLE",
            "fields" => [
              { "name" => "first_key", "type" => "STRING", "mode" => "NULLABLE" },
              { "name" => "second_key", "type" => "STRING", "mode" => "REQUIRED" }
            ]
          }
        ]
      }
    end

    context "when vector configuration exists" do
      before do
        Pathname.pwd.join("vectors").mkpath
        config_path = Pathname.pwd.join("vectors", "#{vector_name}.yml")
        config_path.write(YAML.dump(vector_config))
      end

      it "loads and transforms vector schema" do
        expect(service.load_vector_schema(vector_name)).to eq(expected_schema)
      end

      it "handles uppercase vector names" do
        expect(service.load_vector_schema(vector_name.upcase)).to eq(expected_schema)
      end
    end

    context "when vector configuration doesn't exist" do
      before do
        allow(logger).to receive(:error)
      end

      it "raises an error" do
        expect { service.load_vector_schema(vector_name) }.to raise_error(
          "Vector configuration not found: #{Pathname.pwd.join("vectors", "#{vector_name}.yml")}"
        )
      end
    end

    context "when vector configuration is invalid" do
      before do
        Pathname.pwd.join("vectors").mkpath
        config_path = Pathname.pwd.join("vectors", "#{vector_name}.yml")
        config_path.write("invalid_key: [value1, value2")
      end

      it "raises an error" do
        expect { service.load_vector_schema(vector_name) }.to raise_error(
          /Invalid YAML in vector configuration/
        )
      end
    end
  end

  describe "#load_vector_config" do
    let(:vector_name) { "page" }
    let(:vector_config) do
      {
        "attributes" => {
          "url" => "string",
          "title" => "string"
        },
        "merge" => {
          "source" => "lib/routines/select_pages.sql"
        }
      }
    end

    let(:expected_config) do
      vector_config.merge("name" => vector_name)
    end

    context "when vector configuration exists" do
      before do
        Pathname.pwd.join("vectors").mkpath
        config_path = Pathname.pwd.join("vectors", "#{vector_name}.yml")
        config_path.write(YAML.dump(vector_config))
      end

      it "loads the complete vector configuration" do
        expect(service.load_vector_config(vector_name)).to eq(expected_config)
      end

      it "handles uppercase vector names" do
        expect(service.load_vector_config(vector_name.upcase)).to eq(expected_config)
      end
    end

    context "when vector configuration doesn't exist" do
      it "raises an error" do
        expect { service.load_vector_config(vector_name) }.to raise_error(
          "Vector configuration not found: #{Pathname.pwd.join("vectors", "#{vector_name}.yml")}"
        )
      end
    end

    context "when vector configuration is invalid" do
      before do
        Pathname.pwd.join("vectors").mkpath
        config_path = Pathname.pwd.join("vectors", "#{vector_name}.yml")
        config_path.write("invalid_key: [value1, value2")
      end

      it "raises an error" do
        expect { service.load_vector_config(vector_name) }.to raise_error(
          /Invalid YAML in vector configuration/
        )
      end
    end
  end
end
