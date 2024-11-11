# frozen_string_literal: true

require "manifold"
require "debug"
require "logger"
require "simplecov"
require "simplecov-json"
require "simplecov-lcov"
require "fakefs/spec_helpers"
Dir[File.join(__dir__, "support", "**", "*.rb")].sort.each { |f| require f }

SimpleCov::Formatter::LcovFormatter.config do |c|
  c.report_with_single_file = true
  c.single_report_path = "coverage/lcov.info"
end

SimpleCov.formatters = SimpleCov::Formatter::MultiFormatter.new([
                                                                  SimpleCov::Formatter::HTMLFormatter,
                                                                  SimpleCov::Formatter::JSONFormatter,
                                                                  SimpleCov::Formatter::LcovFormatter
                                                                ])

SimpleCov.start do
  # Set the minimum coverage percentage
  minimum_coverage 95
  minimum_coverage_by_file 90

  # Fail the build if coverage drops below threshold
  refuse_coverage_drop
end

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.example_status_persistence_file_path = "spec/examples.txt"

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.order = :random
end
