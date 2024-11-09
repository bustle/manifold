# frozen_string_literal: true

RSpec.shared_context "with template files" do
  before do
    template_dir.mkpath

    File.write(workspace_template_path, "vectors:\nmetrics:")
    File.write(vector_template_path, "attributes:")
  end

  def template_dir
    Pathname.new(File.dirname(__FILE__)).join("../../../lib/manifolds/templates")
  end

  def vector_template_path
    template_dir.join("vector_template.yml")
  end

  def workspace_template_path
    template_dir.join("workspace_template.yml")
  end
end
