# frozen_string_literal: true

require "json"
require "logger"
require "pathname"
require "thor"
require "yaml"

Dir[File.join(__dir__, "manifold", "**", "*.rb")].each do |file|
  require file
end

module Manifold
  class Error < StandardError; end
end
