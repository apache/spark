#!/usr/bin/env python3

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
# This script generates llm.txt file for Apache Spark documentation

import os
import re
import sys
import argparse
from pathlib import Path
from typing import List, Tuple, Dict, Optional


def extract_title_from_md(file_path: Path) -> str:
    """
    Extract the title from a markdown file by looking for Jekyll front matter.
    Falls back to first H1 header, then to filename if neither found.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # First, try to extract from Jekyll front matter
            if content.startswith('---'):
                front_matter_end = content.find('---', 3)
                if front_matter_end != -1:
                    front_matter = content[3:front_matter_end]
                    # Look for displayTitle first, then title
                    for line in front_matter.split('\n'):
                        if line.startswith('displayTitle:'):
                            title = line[13:].strip().strip('"').strip("'")
                            if title:
                                return title
                        elif line.startswith('title:'):
                            title = line[6:].strip().strip('"').strip("'")
                            if title and title != "Overview":  # Skip generic "Overview"
                                return title
            
            # Then try to find first H1 header after front matter
            lines = content.split('\n')
            for line in lines:
                if line.startswith('# '):
                    title = line[2:].strip()
                    if title and not title.startswith('{'):  # Skip Jekyll variables
                        return title
    except Exception:
        pass
    
    # Fallback to filename-based title
    filename = file_path.stem
    # Convert filename to title (e.g., building-spark -> Building Spark)
    title = filename.replace('-', ' ').replace('_', ' ')
    return title.title()


def parse_index_md(index_path: Path) -> Dict[str, List[Tuple[str, str]]]:
    """
    Parse the index.md file to extract the documentation structure.
    Returns a dictionary with section names as keys and lists of (title, link) tuples as values.
    """
    structure = {}
    current_section = None
    
    try:
        with open(index_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Skip front matter
            if content.startswith('---'):
                front_matter_end = content.find('---', 3)
                if front_matter_end != -1:
                    content = content[front_matter_end + 3:]
            
            lines = content.split('\n')
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                
                # Detect main sections
                if line == "**Programming Guides:**":
                    current_section = "Programming Guides"
                    structure[current_section] = []
                elif line == "**API Docs:**":
                    current_section = "API Docs"
                    structure[current_section] = []
                elif line == "**Deployment Guides:**":
                    current_section = "Deployment Guides"
                    structure[current_section] = []
                elif line == "**Other Documents:**":
                    current_section = "Other Documents"
                    structure[current_section] = []
                elif line == "**External Resources:**":
                    current_section = "External Resources"
                    structure[current_section] = []
                # Parse links in the current section
                elif current_section and line.startswith('*'):
                    # Extract markdown links [title](url)
                    link_pattern = r'\[([^\]]+)\]\(([^\)]+)\)'
                    matches = re.findall(link_pattern, line)
                    for title, url in matches:
                        # Only include internal documentation links
                        if not url.startswith('http') and url.endswith('.html'):
                            structure[current_section].append((title, url))
                        elif url.startswith('api/'):
                            # Include API doc links
                            structure[current_section].append((title, url))
                
                i += 1
    except Exception as e:
        print(f"Warning: Could not parse index.md: {e}", file=sys.stderr)
    
    return structure


def get_all_docs_files(docs_path: Path) -> Dict[str, Tuple[str, str]]:
    """
    Get all documentation files and their titles.
    Returns a dictionary with filename (without .md) as key and (title, path) as value.
    """
    docs_files = {}
    
    # Process root level markdown files
    for md_file in docs_path.glob("*.md"):
        if md_file.name in ["README.md", "404.md"]:
            continue
        filename = md_file.stem
        title = extract_title_from_md(md_file)
        docs_files[filename] = (title, str(md_file.relative_to(docs_path)))
    
    # Process subdirectory markdown files (e.g., streaming/)
    for subdir in docs_path.iterdir():
        if subdir.is_dir() and not subdir.name.startswith('_') and subdir.name not in ['css', 'img', 'js', 'api']:
            for md_file in subdir.glob("*.md"):
                if md_file.name == "README.md":
                    continue
                # Use relative path as key
                rel_path = md_file.relative_to(docs_path)
                filename = str(rel_path.with_suffix(''))
                title = extract_title_from_md(md_file)
                docs_files[filename] = (title, str(rel_path))
    
    return docs_files


def generate_llm_txt(docs_path: Path, output_path: Path, version: str = "latest") -> None:
    """
    Generate the llm.txt file for Apache Spark documentation using structure from index.md.
    """
    # Parse the index.md file to get the documentation structure
    index_path = docs_path / "index.md"
    structure = parse_index_md(index_path)
    
    # Get all available documentation files
    all_docs = get_all_docs_files(docs_path)
    
    # Generate the content
    content = []
    content.append("# Apache Spark")
    content.append("")
    content.append("> Apache Spark™ is a unified analytics engine for large-scale data processing. "
                  "It provides high-level APIs in Java, Scala, Python, and R, and an optimized engine "
                  "that supports general execution graphs. It also supports a rich set of higher-level "
                  "tools including Spark SQL for SQL and structured data processing, MLlib for machine "
                  "learning, GraphX for graph processing, and Structured Streaming for incremental "
                  "computation and stream processing.")
    content.append("")
    
    # Main documentation link
    doc_home_url = f"https://spark.apache.org/docs/{version}/" if version != "latest" else "https://spark.apache.org/docs/latest/"
    content.append(f"Documentation home: {doc_home_url}")
    content.append("")
    
    # Process each section from index.md
    sections_to_include = ["Programming Guides", "API Docs", "Deployment Guides", "Other Documents"]
    
    for section in sections_to_include:
        if section in structure and structure[section]:
            content.append(f"## {section}")
            content.append("")
            
            for title, link in structure[section]:
                # Convert relative links to absolute URLs
                if link.startswith('api/'):
                    # API documentation links
                    url = f"https://spark.apache.org/docs/{version}/{link}"
                elif link.endswith('.html'):
                    # Regular documentation links
                    base_name = link.replace('.html', '')
                    
                    # Handle special cases like streaming/index.html
                    if '/' in base_name:
                        url = f"https://spark.apache.org/docs/{version}/{link}"
                    else:
                        url = f"https://spark.apache.org/docs/{version}/{link}"
                else:
                    # External links (shouldn't happen in these sections)
                    continue
                
                content.append(f"- [{title}]({url})")
            content.append("")
    
    # Add main SQL reference pages (not every single SQL command)
    sql_refs = []
    # Only include high-level SQL reference pages, not individual commands
    important_sql_refs = [
        'sql-ref',  # Main SQL reference
        'sql-ref-ansi-compliance',  # ANSI compliance
        'sql-ref-datatypes',  # Data types
        'sql-ref-datetime-pattern',  # Datetime patterns
        'sql-ref-functions',  # Functions overview
        'sql-ref-functions-builtin',  # Built-in functions
        'sql-ref-identifier',  # Identifiers
        'sql-ref-literals',  # Literals
        'sql-ref-null-semantics',  # NULL semantics
        'sql-ref-syntax',  # SQL syntax overview
    ]
    
    for filename, (title, _) in all_docs.items():
        if filename in important_sql_refs:
            url = f"https://spark.apache.org/docs/{version}/{filename}.html"
            sql_refs.append((title, url))
    
    if sql_refs:
        content.append("## SQL Reference")
        content.append("")
        for title, url in sorted(sql_refs):
            content.append(f"- [{title}]({url})")
        content.append("")
    
    # Add streaming documentation from the streaming/ subdirectory
    streaming_docs = []
    for filename, (title, _) in all_docs.items():
        if filename.startswith('streaming/'):
            url = f"https://spark.apache.org/docs/{version}/{filename}.html"
            streaming_docs.append((title, url))
    
    if streaming_docs:
        content.append("## Structured Streaming (Detailed)")
        content.append("")
        for title, url in sorted(streaming_docs):
            content.append(f"- [{title}]({url})")
        content.append("")
    
    # External Resources
    content.append("## External Resources")
    content.append("")
    content.append("- [Apache Spark Home](https://spark.apache.org/)")
    content.append("- [Downloads](https://spark.apache.org/downloads.html)")
    content.append("- [GitHub Repository](https://github.com/apache/spark)")
    content.append("- [Issue Tracker (JIRA)](https://issues.apache.org/jira/projects/SPARK)")
    content.append("- [Mailing Lists](https://spark.apache.org/mailing-lists.html)")
    content.append("- [Community](https://spark.apache.org/community.html)")
    content.append("- [Contributing](https://spark.apache.org/contributing.html)")
    content.append("")
    
    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(content))
    
    print(f"Generated {output_path}")
    
    # Count total docs included
    total_docs = sum(len(v) for v in structure.values())
    total_docs += len(sql_refs) + len(streaming_docs)
    print(f"Total documentation pages indexed: {total_docs}")
    print(f"Sections: {len([s for s in structure if structure[s]]) + (1 if sql_refs else 0) + (1 if streaming_docs else 0)}")


def main():
    parser = argparse.ArgumentParser(description='Generate llm.txt file for Apache Spark documentation')
    parser.add_argument(
        '--docs-path',
        type=str,
        default='docs',
        help='Path to the docs directory (default: docs)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='llm.txt',
        help='Output file path (default: llm.txt)'
    )
    parser.add_argument(
        '--version',
        type=str,
        default='latest',
        help='Spark documentation version (default: latest)'
    )
    
    args = parser.parse_args()
    
    # Convert to Path objects
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent  # Go up two levels from dev/create-release/
    docs_path = project_root / args.docs_path
    output_path = project_root / args.output
    
    # Check if docs directory exists
    if not docs_path.exists():
        print(f"Error: Documentation directory '{docs_path}' does not exist")
        sys.exit(1)
    
    # Generate the llm.txt file
    generate_llm_txt(docs_path, output_path, args.version)


if __name__ == "__main__":
    main()