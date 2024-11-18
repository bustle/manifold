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

      self.bq_service = Services::BigQueryService.new(logger)
    end

    desc "init NAME", "Generate a new umbrella project for data management"
    def init(name)
      Manifold::API::Project.create(name)
      logger.info "Created umbrella project '#{name}' with projects and vectors directories."
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

    desc "generate PROJECT_NAME SERVICE", "Generate services for a project"
    def generate(project_name, service)
      case service
      when "bq"
        bq_service.generate_dimensions_schema(project_name)
      else
        logger.error("Unsupported service: #{service}")
      end
    end
  end
end
