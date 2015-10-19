require 'liquid'
require 'pygments'

module Jekyll
  class IncludeExampleTag < Liquid::Tag
    
    def initialize(tag_name, markup, tokens)
      @markup = markup
      super
    end
 
    def render(context)
      site = context.registers[:site]
      config_dir = (site.config['code_dir'] || '../examples/src/main').sub(/^\//,'')
      @code_dir = File.join(site.source, config_dir)

      clean_markup = @markup.strip
      @file = File.join(@code_dir, clean_markup)
      @lang = clean_markup.split('.').last

      code = File.open(@file).read.encode("UTF-8")
      code = select_lines(code)
 
      Pygments.highlight(code, :lexer => @lang)
    end
 
    # Select lines according to labels in code. Currently we use "begin code" and "end code" as
    # labels.
    def select_lines(code)
      lines = code.each_line.to_a
      start = lines.each_with_index.select { |l, i| l.include? "$example on$" }.last.last
      endline = lines.each_with_index.select { |l, i| l.include? "$example off$" }.last.last
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

Liquid::Template.register_tag('include_example', Jekyll::IncludeExampleTag)
