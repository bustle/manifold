# frozen_string_literal: true

RSpec.describe Manifold::Terraform::SQLBuilder do
  subject(:builder) { described_class.new(name, manifold_config) }

  let(:name) { "analytics" }
  let(:manifold_config) do
    {
      "source" => {
        "table" => "bdg-wetland.EventStream.CardTaps",
        "id_field" => "dimensions.cardZoneId",
        "lookback" => "90 DAY"
      },
      "timestamp" => {
        "field" => "timestamp",
        "interval" => "DAY"
      }
    }
  end

  describe "#build_manifold_merge_sql" do
    subject(:sql) { builder.build_manifold_merge_sql(metrics_builder) { metrics_struct } }

    let(:metrics_builder) { instance_double(Manifold::Terraform::MetricsBuilder) }
    let(:metrics_struct) { "STRUCT(COUNTIF(IS_PAID(context.location)) AS tapCount) AS paid" }

    it "includes the source table" do
      expect(sql).to include("FROM `bdg-wetland.EventStream.CardTaps`")
    end

    it "uses the configured id field" do
      expect(sql).to include("dimensions.cardZoneId id")
    end

    it "uses the configured timestamp field and interval" do
      expect(sql).to include("TIMESTAMP_TRUNC(timestamp, DAY) timestamp")
    end

    it "uses the configured lookback" do
      expect(sql).to include("INTERVAL 90 DAY")
    end

    it "includes the metrics struct" do
      expect(sql).to include(metrics_struct)
    end

    context "with default configuration" do
      let(:manifold_config) do
        {
          "source" => { "table" => "bdg-wetland.EventStream.CardTaps" },
          "timestamp" => { "field" => "timestamp" }
        }
      end

      it "uses default id field" do
        expect(sql).to include("dimensions.id id")
      end

      it "uses default interval" do
        expect(sql).to include("TIMESTAMP_TRUNC(timestamp, DAY)")
      end

      it "uses default lookback" do
        expect(sql).to include("INTERVAL 90 DAY")
      end
    end
  end

  describe "#build_dimensions_merge_sql" do
    subject(:sql) { builder.build_dimensions_merge_sql(source_sql) }

    let(:source_sql) { "SELECT id, STRUCT(url, title) AS dimensions FROM pages" }

    it "includes the source SQL" do
      expect(sql).to include(source_sql)
    end

    it "merges into the dimensions table" do
      expect(sql).to include("MERGE #{name}.Dimensions AS TARGET")
    end

    it "updates dimensions on match" do
      expect(sql).to include("WHEN MATCHED THEN UPDATE SET target.dimensions = source.dimensions")
    end

    it "inserts new rows" do
      expect(sql).to include("WHEN NOT MATCHED THEN INSERT ROW")
    end
  end
end
