# frozen_string_literal: true

require "pathname"
require "thor"
require "yaml"
require "logger"

Dir[File.join(__dir__, "manifold", "**", "*.rb")].sort.each do |file|
  require file
end

module Manifold
  class Error < StandardError; end
end
