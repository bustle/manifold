# frozen_string_literal: true

RSpec.describe Manifold::Terraform::Configuration do
  include FakeFS::SpecHelpers

  let(:test_configuration) do
    Class.new(described_class) do
      def as_json
        { "test" => "config" }
      end
    end.new
  end

  describe "#write" do
    let(:path) { Pathname.new("test.tf.json") }

    it "writes pretty-printed JSON to the specified path" do
      test_configuration.write(path)
      expect(path.read).to eq(expected_json)
    end

    def expected_json
      <<~JSON
        {
          "test": "config"
        }
      JSON
    end
  end
end
