# frozen_string_literal: true

require "pathname"
require "thor"
require "yaml"

Dir[File.join(__dir__, "manifolds", "**", "*.rb")].sort.each do |file|
  require file
end

module Manifolds
  class Error < StandardError; end
end
