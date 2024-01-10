module Jekyll
  # Tag for including a file if it exists.
  class IncludeRelativeIfExistsTag < Tags::IncludeRelativeTag
    def render(context)
      super
    rescue IOError
      "<!-- missing include: #{@file} -->"
    end
  end
  
  # Tag for including files generated as part of the various language APIs.
  # If a SKIP_ flag is set, tolerate missing files. If not, raise an error.
  class IncludeApiGenTag < Tags::IncludeRelativeTag
    @@displayed_warning = false

    def render(context)
      super
    rescue IOError => e
      skip_flags = [
        'SKIP_API',
        'SKIP_SCALADOC',
        'SKIP_PYTHONDOC',
        'SKIP_RDOC',
        'SKIP_SQLDOC',
      ]
      set_flags = skip_flags.select { |flag| ENV[flag] }
      # A more sophisticated approach would be to accept a tag parameter
      # specifying the relevant API so we tolerate missing files only for
      # APIs that are explicitly skipped. But this is unnecessary for now.
      # Instead, we simply tolerate missing files if _any_ skip flag is set.
      if set_flags.any? then
        set_flags_string = set_flags.join(', ')
        if !@@displayed_warning then
          STDERR.puts "Warning: Tolerating missing API files because the " \
            "following skip flags are set: #{set_flags_string}"
          @@displayed_warning = true
        end
        # "skip flags set: `#{set_flags_string}`; " \
        "placeholder for missing API include: `#{@file}`"
      else
        raise e
      end
    end
  end
end

Liquid::Template.register_tag(
  'include_relative_if_exists',
  Jekyll::IncludeRelativeIfExistsTag,
)

Liquid::Template.register_tag(
  'include_api_gen',
  Jekyll::IncludeApiGenTag,
)
