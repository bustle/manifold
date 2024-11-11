# frozen_string_literal: true

RSpec.shared_context "with template files" do
  before do
    template_dir.mkpath

    workspace_template_path.write("vectors:\nmetrics:")
    vector_template_path.write("attributes:")
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
