# frozen_string_literal: true

require_relative "lib/manifold/version"

Gem::Specification.new do |spec|
  spec.name = "manifold-cli"
  spec.version = Manifold::VERSION
  spec.authors = ["claytongentry"]
  spec.email = ["clayton@bustle.com"]

  spec.summary = "A CLI for managing data infrastructures in BigQuery"
  spec.homepage = "https://github.com/bustle/manifold"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/bustle/manifold"
  spec.metadata["changelog_uri"] = "https://github.com/bustle/manifold/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end

  spec.executables << "manifold"
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  spec.add_dependency "thor"
  spec.metadata["rubygems_mfa_required"] = "true"
end
