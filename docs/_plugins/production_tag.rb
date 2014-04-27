module Jekyll
  class ProductionTag < Liquid::Block

    def initialize(tag_name, markup, tokens)
      super
    end

    def render(context)
      if ENV['PRODUCTION'] then super else "" end
    end
  end
end

Liquid::Template.register_tag('production', Jekyll::ProductionTag)
