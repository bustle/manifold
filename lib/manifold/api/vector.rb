# frozen_string_literal: true

module Manifold
  module API
    # Describes the entities for whom metrics are calculated.
    class Vector
      attr_reader :name, :template_path

      DEFAULT_TEMPLATE_PATH = File.expand_path(
        "../templates/vector_template.yml", __dir__
      ).freeze

      def initialize(name, template_path: DEFAULT_TEMPLATE_PATH)
        @name = name
        @template_path = Pathname(template_path)
      end

      def add
        directory.mkpath
        FileUtils.cp(template_path, config_path)
      end

      private

      def directory
        Pathname.pwd.join("vectors")
      end

      def config_path
        directory.join("#{name.downcase}.yml")
      end
    end
  end
end
