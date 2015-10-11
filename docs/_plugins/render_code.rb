require 'octopress-code-highlighter'
require 'liquid'
require 'colorator'

module Octopress
  module RenderCode
    class Tag < Liquid::Tag

      FileOnly = /^(\S+)$/
      FileTitle = /(\S+)\s+(\S.*?)$/i
      TitleFile = /(\S.*?)\s+(\S+)$/i

      def initialize(tag_name, markup, tokens)
        @markup = markup
        super
      end

      def render(context)
        if page = context.environments.first['page']
          @page_path = page['path']
        end
        site = context.registers[:site]
        config_dir = (site.config['code_dir'] || 'downloads/code').sub(/^\//,'')
        @code_dir = File.join(site.source, config_dir)

        begin
          options = get_options
          code = File.open(@file).read.encode("UTF-8")
          code = CodeHighlighter.select_lines(code, options)

          highlight(code, options)
        rescue => e
          failure(e)
        end
      end

      def get_path(file)
        path = File.join(@code_dir, file)

        if File.symlink?(path)
          raise "Code directory '#{@code_path}' cannot be a symlink"
        end

        path if File.exists?(path)
      end

      def get_options
        # wrap with {% raw %} tags to prevent addititional processing
        defaults = { }

        clean_markup = CodeHighlighter.clean_markup(@markup).strip

        if clean_markup =~ FileOnly
          @file = get_path($1)
        elsif clean_markup =~ FileTitle
          if @file = get_path($1)
            defaults[:title] = $2
          
          # Allow for deprecated title first syntax
          #
          elsif clean_markup =~ TitleFile
            defaults[:title] = $1
            @file = get_path($2)
            if @page_path
              puts "\nRenderCode Warning:".red
              puts "  Passing title before path has been deprecated and will be removed in RenderCode 2.0".red
              puts "  Update #{@page_path} with {% render_code #{$2} #{$1} ... %}.".yellow
            end
          end
        end

        options = CodeHighlighter.parse_markup(@markup, defaults)
        options[:lang] ||= File.extname(@file).delete('.')
        options[:link_text] ||= "Raw code"
        options
      end

      def failure(message)
        CodeHighlighter.highlight_failed(message, "{% include_code path/to/file [title] [lang:language] [start:#] [end:#] [range:#-#] [mark:#,#-#] [linenos:false] %}", @markup, '')
      end


      def highlight(code, options)
        options[:aliases] = @aliases || {}
        code = CodeHighlighter.highlight(code, options)
        if @page_path
          code = "<notextile>#{code}</notextile>" if File.extname(@page_path).match(/textile/)
        end
        code
      end
    end
  end
end

Liquid::Template.register_tag('render_code', Octopress::RenderCode::Tag)
