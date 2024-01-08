module Jekyll
  class IncludeRelativeIfExistsTag < Tags::IncludeRelativeTag
    def render(context)
      super
    rescue IOError => e
      puts e
      "placeholder for missing file: `#{@file}`"
    end
  end
end

Liquid::Template.register_tag(
  'include_relative_if_exists',
  Jekyll::IncludeRelativeIfExistsTag,
)
