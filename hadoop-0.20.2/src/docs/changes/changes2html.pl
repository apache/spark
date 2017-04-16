#!/usr/bin/perl
#
# Transforms Lucene Java's CHANGES.txt into Changes.html
#
# Input is on STDIN, output is to STDOUT
#
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

use strict;
use warnings;

my $jira_url_prefix = 'http://issues.apache.org/jira/browse/';
my $title = undef;
my $release = undef;
my $sections = undef;
my $items = undef;
my $first_relid = undef;
my $second_relid = undef;
my @releases = ();

my @lines = <>;                        # Get all input at once

#
# Parse input and build hierarchical release structure in @releases
#
for (my $line_num = 0 ; $line_num <= $#lines ; ++$line_num) {
  $_ = $lines[$line_num];
  next unless (/\S/);                  # Skip blank lines

  unless ($title) {
    if (/\S/) {
      s/^\s+//;                        # Trim leading whitespace
      s/\s+$//;                        # Trim trailing whitespace
    }
    $title = $_;
    next;
  }

  if (/^(Release)|(Trunk)/) {   # Release headings
    $release = $_;
    $sections = [];
    push @releases, [ $release, $sections ];
    ($first_relid = lc($release)) =~ s/\s+/_/g   if ($#releases == 0);
    ($second_relid = lc($release)) =~ s/\s+/_/g  if ($#releases == 1);
    $items = undef;
    next;
  }

  # Section heading: 2 leading spaces, words all capitalized
  if (/^  ([A-Z]+)\s*/) {
    my $heading = $_;
    $items = [];
    push @$sections, [ $heading, $items ];
    next;
  }

  # Handle earlier releases without sections - create a headless section
  unless ($items) {
    $items = [];
    push @$sections, [ undef, $items ];
  }

  my $type;
  if (@$items) { # A list item has been encountered in this section before
    $type = $items->[0];  # 0th position of items array is list type
  } else {
    $type = get_list_type($_);
    push @$items, $type;
  }

  if ($type eq 'numbered') { # The modern items list style
    # List item boundary is another numbered item or an unindented line
    my $line;
    my $item = $_;
    $item =~ s/^(\s{0,2}\d+\.\s*)//;       # Trim the leading item number
    my $leading_ws_width = length($1);
    $item =~ s/\s+$//;                     # Trim trailing whitespace
    $item .= "\n";

    while ($line_num < $#lines
           and ($line = $lines[++$line_num]) !~ /^(?:\s{0,2}\d+\.\s*\S|\S)/) {
      $line =~ s/^\s{$leading_ws_width}//; # Trim leading whitespace
      $line =~ s/\s+$//;                   # Trim trailing whitespace
      $item .= "$line\n";
    }
    $item =~ s/\n+\Z/\n/;                  # Trim trailing blank lines
    push @$items, $item;
    --$line_num unless ($line_num == $#lines);
  } elsif ($type eq 'paragraph') {         # List item boundary is a blank line
    my $line;
    my $item = $_;
    $item =~ s/^(\s+)//;
    my $leading_ws_width = defined($1) ? length($1) : 0;
    $item =~ s/\s+$//;                     # Trim trailing whitespace
    $item .= "\n";

    while ($line_num < $#lines and ($line = $lines[++$line_num]) =~ /\S/) {
      $line =~ s/^\s{$leading_ws_width}//; # Trim leading whitespace
      $line =~ s/\s+$//;                   # Trim trailing whitespace
      $item .= "$line\n";
    }
    push @$items, $item;
    --$line_num unless ($line_num == $#lines);
  } else { # $type is one of the bulleted types
    # List item boundary is another bullet or a blank line
    my $line;
    my $item = $_;
    $item =~ s/^(\s*$type\s*)//;           # Trim the leading bullet
    my $leading_ws_width = length($1);
    $item =~ s/\s+$//;                     # Trim trailing whitespace
    $item .= "\n";

    while ($line_num < $#lines
           and ($line = $lines[++$line_num]) !~ /^\s*(?:$type|\Z)/) {
      $line =~ s/^\s{$leading_ws_width}//; # Trim leading whitespace
      $line =~ s/\s+$//;                   # Trim trailing whitespace
      $item .= "$line\n";
    }
    push @$items, $item;
    --$line_num unless ($line_num == $#lines);
  }
}

#
# Print HTML-ified version to STDOUT
#
print<<"__HTML_HEADER__";
<!--
**********************************************************
** WARNING: This file is generated from CHANGES.txt by the 
**          Perl script 'changes2html.pl'.
**          Do *not* edit this file!
**********************************************************
          
****************************************************************************
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
****************************************************************************
-->
<html>
<head>
  <title>$title</title>
  <link rel="stylesheet" href="ChangesFancyStyle.css" title="Fancy">
  <link rel="alternate stylesheet" href="ChangesSimpleStyle.css" title="Simple">
  <META http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <SCRIPT>
    function toggleList(e) {
      element = document.getElementById(e).style;
      element.display == 'none' ? element.display = 'block' : element.display='none';
    }
    function collapse() {
      for (var i = 0; i < document.getElementsByTagName("ul").length; i++) {
        var list = document.getElementsByTagName("ul")[i];
        if (list.id != '$first_relid' && list.id != '$second_relid') {
          list.style.display = "none";
        }
      }
      for (var i = 0; i < document.getElementsByTagName("ol").length; i++) {
        document.getElementsByTagName("ol")[i].style.display = "none"; 
      }
    }
    window.onload = collapse;
  </SCRIPT>
</head>
<body>

<a href="http://hadoop.apache.org/core/"><img class="logoImage" alt="Hadoop" src="images/hadoop-logo.jpg" title="Scalable Computing Platform"></a>
<h1>$title</h1>

__HTML_HEADER__

my $heading;
my $relcnt = 0;
my $header = 'h2';
for my $rel (@releases) {
  if (++$relcnt == 3) {
    $header = 'h3';
    print "<h2><a href=\"javascript:toggleList('older')\">";
    print "Older Releases";
    print "</a></h2>\n";
    print "<ul id=\"older\">\n"
  }
      
  ($release, $sections) = @$rel;

  # The first section heading is undefined for the older sectionless releases
  my $has_release_sections = $sections->[0][0];

  (my $relid = lc($release)) =~ s/\s+/_/g;
  print "<$header><a href=\"javascript:toggleList('$relid')\">";
  print "$release";
  print "</a></$header>\n";
  print "<ul id=\"$relid\">\n"
    if ($has_release_sections);

  for my $section (@$sections) {
    ($heading, $items) = @$section;
    (my $sectid = lc($heading)) =~ s/\s+/_/g;
    my $numItemsStr = $#{$items} > 0 ? "($#{$items})" : "(none)";  

    print "  <li><a href=\"javascript:toggleList('$relid.$sectid')\">",
          ($heading || ''), "</a>&nbsp;&nbsp;&nbsp;$numItemsStr\n"
      if ($has_release_sections);

    my $list_type = $items->[0] || '';
    my $list = ($has_release_sections || $list_type eq 'numbered' ? 'ol' : 'ul');
    my $listid = $sectid ? "$relid.$sectid" : $relid;
    print "    <$list id=\"$listid\">\n";

    for my $itemnum (1..$#{$items}) {
      my $item = $items->[$itemnum];
      $item =~ s:&:&amp;:g;                            # Escape HTML metachars
      $item =~ s:<:&lt;:g; 
      $item =~ s:>:&gt;:g;

      $item =~ s:\s*(\([^)"]+?\))\s*$:<br />$1:;       # Separate attribution
      $item =~ s:\n{2,}:\n<p/>\n:g;                    # Keep paragraph breaks
      $item =~ s{(?:${jira_url_prefix})?(HADOOP-\d+)}  # Link to JIRA
                {<a href="${jira_url_prefix}$1">$1</a>}g;
      print "      <li>$item</li>\n";
    }
    print "    </$list>\n";
    print "  </li>\n" if ($has_release_sections);
  }
  print "</ul>\n" if ($has_release_sections);
}
print "</ul>\n" if ($relcnt > 3);
print "</body>\n</html>\n";


#
# Subroutine: get_list_type
#
# Takes one parameter:
#
#    - The first line of a sub-section/point
#
# Returns one scalar:
#
#    - The list type: 'numbered'; or one of the bulleted types '-', or '.' or
#      'paragraph'.
#
sub get_list_type {
  my $first_list_item_line = shift;
  my $type = 'paragraph'; # Default to paragraph type

  if ($first_list_item_line =~ /^\s{0,2}\d+\.\s+\S+/) {
    $type = 'numbered';
  } elsif ($first_list_item_line =~ /^\s*([-.])\s+\S+/) {
    $type = $1;
  }
  return $type;
}

1;
