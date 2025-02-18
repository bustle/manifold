# frozen_string_literal: true

RSpec.describe Manifold::Terraform::MetricsBuilder do
  subject(:builder) { described_class.new(manifold_config) }

  let(:manifold_config) do
    {
      "breakouts" => {
        "paid" => "IS_PAID(context.location)",
        "organic" => "IS_ORGANIC(context.location)",
        "paidOrganic" => {
          "fields" => %w[paid organic],
          "operator" => "AND"
        },
        "paidOrOrganic" => {
          "fields" => %w[paid organic],
          "operator" => "OR"
        },
        "notPaid" => {
          "fields" => ["paid"],
          "operator" => "NOT"
        },
        "neitherPaidNorOrganic" => {
          "fields" => %w[paid organic],
          "operator" => "NOR"
        },
        "notBothPaidAndOrganic" => {
          "fields" => %w[paid organic],
          "operator" => "NAND"
        },
        "eitherPaidOrOrganic" => {
          "fields" => %w[paid organic],
          "operator" => "XOR"
        },
        "similarPaidOrganic" => {
          "fields" => %w[paid organic],
          "operator" => "XNOR"
        }
      },
      "metrics" => {
        "countif" => "tapCount",
        "sumif" => {
          "sequenceSum" => {
            "field" => "context.sequence"
          }
        }
      }
    }
  end

  describe "#build_metrics_struct" do
    subject(:metrics_struct) { builder.build_metrics_struct }

    context "with valid configuration" do
      it "wraps each context in STRUCT" do
        manifold_config["breakouts"].each_key do |_context|
          expect(metrics_struct).to include("STRUCT(")
        end
      end

      it "includes each context name" do
        manifold_config["breakouts"].each_key do |context|
          expect(metrics_struct).to include(") AS #{context}")
        end
      end

      it "includes countif function" do
        expect(metrics_struct).to include("COUNTIF(")
      end

      it "includes countif metric name" do
        expect(metrics_struct).to include(") AS tapCount")
      end

      it "includes sumif function" do
        expect(metrics_struct).to include("SUM(IF(")
      end

      it "includes sumif field reference" do
        expect(metrics_struct).to include(", context.sequence, 0)")
      end
    end

    context "when no breakouts are defined" do
      let(:manifold_config) { { "metrics" => {} } }

      it { is_expected.to eq("") }
    end

    context "when no metrics are defined" do
      let(:manifold_config) { { "breakouts" => {} } }

      it { is_expected.to eq("") }
    end
  end
end
