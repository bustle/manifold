# frozen_string_literal: true

module Manifold
  module Terraform
    # Provides a base class for Terraform configuration files.
    class Configuration
      def as_json
        raise NotImplementedError, "#{self.class} must implement #as_json"
      end

      def write(path)
        path.write("#{JSON.pretty_generate(as_json)}\n")
      end
    end
  end
end
