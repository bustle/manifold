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

  describe "#build_metric_merge_sql" do
    subject(:sql) { builder.build_metric_merge_sql("renders") }

    let(:manifold_config) do
      {
        "timestamp" => { "field" => "timestamp", "interval" => "DAY" },
        "metrics" => {
          "renders" => {
            "source" => "my_project.events",
            "filter" => "timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)",
            "conditions" => {
              "mobile" => { "args" => { "device" => "STRING" }, "body" => "device = 'mobile'" }
            },
            "aggregations" => {
              "countif" => "renderCount",
              "sumif" => { "seqSum" => { "field" => "context.seq" } }
            }
          }
        }
      }
    end

    it "merges into the renders metrics table" do
      expect(sql).to include("MERGE analytics.RendersMetrics AS target")
    end

    it "truncates the timestamp field by interval" do
      expect(sql).to include("TIMESTAMP_TRUNC(timestamp, DAY) AS timestamp")
    end

    it "uses the configured source" do
      expect(sql).to include("FROM my_project.events")
    end

    it "uses the configured filter" do
      expect(sql).to include("WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)")
    end

    it "groups by id and timestamp" do
      expect(sql).to match(/GROUP BY id, timestamp/)
    end

    it "includes countif aggregation with condition expression" do
      expect(sql).to include("COUNTIF(isMobile(device)) AS renderCount")
    end

    it "includes sumif aggregation with IF expression" do
      expect(sql).to include("SUM(IF(isMobile(device), context.seq, 0)) AS seqSum")
    end

    it "updates on match and inserts on not match" do
      expect(sql).to include("WHEN MATCHED THEN").and include("WHEN NOT MATCHED THEN INSERT ROW")
    end
  end
end
