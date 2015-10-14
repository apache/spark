require 'octopress-code-highlighter'
require 'liquid'

module Octopress
  module IncludeExample
    class Tag < Liquid::Tag
      
      # Pattern for {% include_example /path/to/a/file.lan %}
      FileOnly = /^(\S+)$/
      # Pattern for {% include_example /path/to/a/file.lan title %}
      FileTitle = /(\S+)\s+(\S.*?)$/i

      def initialize(tag_name, markup, tokens)
        @markup = markup
        super
      end

      def render(context)
        site = context.registers[:site]
        config_dir = (site.config['code_dir'] || '../examples/src/main').sub(/^\//,'')
        @code_dir = File.join(site.source, config_dir)

        begin
          options = get_options
          code = File.open(@file).read.encode("UTF-8")
          code = select_lines(code, options)

          CodeHighlighter.highlight(code, options)
        rescue => e
          puts "not work"
        end
      end

      def get_options
        defaults = {}
        clean_markup = CodeHighlighter.clean_markup(@markup).strip

        if clean_markup =~ FileOnly
          @file = File.join(@code_dir, $1)
        elsif clean_markup =~ FileTitle
          @file = File.join(@code_dir, $1)
          defaults[:title] = $2
        end

        options = CodeHighlighter.parse_markup(@markup, defaults)
        options[:lang] ||= File.extname(@file).delete('.')
        options[:link_text] ||= "Example code"
        options
      end

      # Select lines according to labels in code. Currently we use "begin code" and "end code" as
      # labels.
      def select_lines(code, options)
        lines = code.each_line.to_a
        start = lines.each_with_index.select { |l, i| l.include? "begin code" }.last.last
        endline = lines.each_with_index.select { |l, i| l.include? "end code" }.last.last
        length = lines.count

        if start > 0 or endline < length - 1
          raise "Wrong positions of begin and end tags." if start > endline
          range = Range.new(start, endline)
          code = lines[range].join
        end
        code
      end
    end
  end
end

Liquid::Template.register_tag('include_example', Octopress::IncludeExample::Tag)
