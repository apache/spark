require 'octopress-code-highlighter'
require 'liquid'

module Octopress
  module IncludeExample
    class Tag < Liquid::Tag

      def initialize(tag_name, fname, tokens)
        super
        @fname = fname
      end

      def render(context)
        site = context.registers[:site]
        config_dir = (site.config['code_dir'] || '../examples/src/main').sub(/^\//,'')
        code_dir = File.join(site.source, config_dir)
        complete_fname = File.join(code_dir, @fname)

        raw_code = File.open("/Users/panda/git-store/spark/docs/../examples/src/main/scala/org/apache/spark/examples/SparkALS.scala").read.encode("UTF-8")
        # raw_code = File.open(complete_fname).read.encode("UTF-8")
        
        code = select_lines(raw_code)
        puts code
        CodeHighlighter.highlight(code)

        begin
        rescue => e
          $stderr.puts "highlight code failed"
        end
      end

      def select_lines(code)
        lines = code.each_line.to_a
        start = lines.each_with_index.select { |l, i| l.include? "begin code" }.last.last
        endline = lines.each_with_index.select { |l, i| l.include? "end code" }.last.last
        length = lines.count

        if start > 0 or endline < length - 1
          raise "Code is #{length} lines long, cannot begin at line #{start}." if start > length
          raise "Code lines starting line #{start} cannot be after ending line #{endline}." if start > endline
          range = Range.new(start, endline)
          code = lines.to_a[range].join
        end
        code
      end
    end
  end
end

Liquid::Template.register_tag('include_example', Octopress::IncludeExample::Tag)
