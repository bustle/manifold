# frozen_string_literal: true

RSpec.shared_context "with template files" do
  before do
    FileUtils.mkdir_p(template_dir)
    File.write(
      template_dir.join("workspace_template.yml"),
      "vectors:\nmetrics:"
    )
    File.write(
      template_dir.join("vector_template.yml"),
      "attributes:"
    )
  end

  def template_dir
    Pathname.new(File.dirname(__FILE__)).join("../../../lib/manifolds/templates")
  end
end
