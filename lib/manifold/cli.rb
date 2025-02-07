# frozen_string_literal: true

module Manifold
  # CLI provides command line interface functionality
  # for creating and managing umbrella projects for data management.
  class CLI < Thor
    attr_accessor :logger, :bq_service

    def initialize(*args, logger: Logger.new($stdout))
      super(*args)

      self.logger = logger
      logger.level = Logger::INFO
    end

    desc "init NAME", "Generate a new umbrella project for data management"
    def init(name)
      Manifold::API::Project.create(name)
      logger.info "Created umbrella project '#{name}' with workspaces and vectors directories."
    end

    desc "vectors SUBCOMMAND ...ARGS", "Manage vectors"
    subcommand "vectors", Class.new(Thor) {
      namespace :vectors

      attr_accessor :logger

      def initialize(*args, logger: Logger.new($stdout))
        super(*args)
        self.logger = logger
      end

      desc "add VECTOR_NAME", "Add a new vector configuration"
      def add(name)
        vector = API::Vector.new(name)
        vector.add
        logger.info "Created vector configuration for '#{name}'."
      end
    }

    desc "add WORKSPACE_NAME", "Add a new workspace to a project"
    def add(name)
      workspace = API::Workspace.new(name)
      workspace.add
      logger.info "Added workspace '#{name}' with tables and routines directories."
    end

    desc "generate", "Generate BigQuery schema for all workspaces in the project"
    method_option :tf, type: :boolean, desc: "Generate Terraform configurations"
    method_option :submodule, type: :boolean, default: false,
                              desc: "Generate Terraform configurations as a submodule (skips provider configuration)"
    def generate
      path = Pathname.pwd
      name = path.basename.to_s
      project = API::Project.new(name, directory: path, logger:)
      project.generate(with_terraform: options[:tf], is_submodule: options[:submodule])
      logger.info "Generated BigQuery schema for all workspaces in the project."
    end
  end
end
