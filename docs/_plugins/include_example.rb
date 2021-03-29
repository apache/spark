#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'liquid'
require 'rouge'

module Jekyll
  class IncludeExampleTag < Liquid::Tag

    def initialize(tag_name, markup, tokens)
      @markup = markup
      super
    end

    def render(context)
      site = context.registers[:site]
      config_dir = '../examples/src/main'
      @code_dir = File.join(site.source, config_dir)

      clean_markup = @markup.strip

      parts = clean_markup.strip.split(' ')
      if parts.length > 1 then
        @snippet_label = ':' + parts[0]
        snippet_file = parts[1]
      else
        @snippet_label = ''
        snippet_file = parts[0]
      end

      @file = File.join(@code_dir, snippet_file)
      @lang = snippet_file.split('.').last

      begin
        code = File.open(@file).read.encode("UTF-8")
      rescue => e
        # We need to explicitly exit on exceptions here because Jekyll will silently swallow
        # them, leading to silent build failures (see https://github.com/jekyll/jekyll/issues/5104)
        puts(e)
        puts(e.backtrace)
        exit 1
      end
      code = select_lines(code).strip

      formatter = Rouge::Formatters::HTMLPygments.new(Rouge::Formatters::HTML.new)
      lexer = Rouge::Lexer.find(@lang)
      rendered_code = formatter.format(lexer.lex(code))

      hint = "<div><small>Find full example code at " \
        "\"examples/src/main/#{snippet_file}\" in the Spark repo.</small></div>"

      rendered_code + hint
    end

    # Trim the code block so as to have the same indentation, regardless of their positions in the
    # code file.
    def trim_codeblock(lines)
      # Select the minimum indentation of the current code block.
      min_start_spaces = lines
        .select { |l| l.strip.size !=0 }
        .map { |l| l[/\A */].size }
        .min

      lines.map { |l| l.strip.size == 0 ? l : l[min_start_spaces .. -1] }
    end

    # Select lines according to labels in code. Currently we use "$example on$" and "$example off$"
    # as labels. Note that code blocks identified by the labels should not overlap.
    def select_lines(code)
      lines = code.each_line.to_a

      # Select the array of start labels from code.
      startIndices = lines
        .each_with_index
        .select { |l, i| l.include? "$example on#{@snippet_label}$" }
        .map { |l, i| i }

      # Select the array of end labels from code.
      endIndices = lines
        .each_with_index
        .select { |l, i| l.include? "$example off#{@snippet_label}$" }
        .map { |l, i| i }

      raise "Start indices amount is not equal to end indices amount, see #{@file}." \
        unless startIndices.size == endIndices.size

      raise "No code is selected by include_example, see #{@file}." \
        if startIndices.size == 0

      # Select and join code blocks together, with a space line between each of two continuous
      # blocks.
      lastIndex = -1
      result = ""
      startIndices.zip(endIndices).each do |start, endline|
        raise "Overlapping between two example code blocks are not allowed, see #{@file}." \
            if start <= lastIndex
        raise "$example on$ should not be in the same line with $example off$, see #{@file}." \
            if start == endline
        lastIndex = endline
        range = Range.new(start + 1, endline - 1)
        trimmed = trim_codeblock(lines[range])
        # Filter out possible example tags of overlapped labels.
        taggs_filtered = trimmed.select { |l| !l.include? '$example ' }
        result += taggs_filtered.join
        result += "\n"
      end
      result
    end
  end
end

Liquid::Template.register_tag('include_example', Jekyll::IncludeExampleTag)
