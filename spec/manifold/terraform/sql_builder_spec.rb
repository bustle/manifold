# frozen_string_literal: true

RSpec.describe Manifold::Terraform::SQLBuilder do
  subject(:builder) { described_class.new(name, manifold_config) }

  let(:name) { "analytics" }
  let(:manifold_config) do
    {
      "timestamp" => {
        "field" => "timestamp",
        "interval" => "DAY"
      },
      "metrics" => {
        "taps" => {
          "source" => "my_project.render_metrics",
          "filter" => "timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)",
          "breakouts" => {
            "paid" => "IS_PAID(context.location)"
          },
          "aggregations" => {
            "countif" => "tapCount"
          }
        }
      }
    }
  end

  describe "#build_dimensions_merge_sql" do
    subject(:sql) { builder.build_dimensions_merge_sql(source_sql) }

    let(:source_sql) do
      <<~SQL
        SELECT
          id,
          STRUCT(
            (SELECT AS STRUCT Cards.*) AS card
          ) AS dimensions
        FROM my_project.my_cards
      SQL
    end

    it "merges into the dimensions table" do
      expect(sql).to include("MERGE #{name}.Dimensions AS TARGET")
    end

    it "includes the source SQL" do
      expect(sql).to include("(SELECT AS STRUCT Cards.*) AS card")
    end

    it "updates dimensions on match" do
      expect(sql).to include("WHEN MATCHED THEN UPDATE SET target.dimensions = source.dimensions")
    end

    it "inserts new rows" do
      expect(sql).to include("WHEN NOT MATCHED THEN INSERT ROW")
    end
  end
end
