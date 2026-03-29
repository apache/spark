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
# This script generates llms.txt file for Apache Spark documentation

import sys
import argparse
from pathlib import Path


def generate_llms_txt(docs_path: Path, output_path: Path, version: str = "latest") -> None:
    """
    Generate the llms.txt file for Apache Spark documentation with hardcoded categories.
    """
    content = []
    content.append("# Apache Spark")
    content.append("")
    content.append(
        "> Apache Spark™ is a unified analytics engine for large-scale data processing. "
        "It provides high-level APIs in Java, Scala, Python, and R, and an optimized engine "
        "that supports general execution graphs. It also supports a rich set of higher-level "
        "tools including Spark SQL for SQL and structured data processing, MLlib for machine "
        "learning, GraphX for graph processing, and Structured Streaming for incremental "
        "computation and stream processing."
    )
    content.append("")

    doc_home_url = f"https://spark.apache.org/docs/{version}/"
    content.append(f"Documentation home: {doc_home_url}")
    content.append("")

    content.append("## Programming Guides")
    content.append("")
    programming_guides = [
        ("Quick Start", f"https://spark.apache.org/docs/{version}/quick-start.html"),
        ("Spark SQL", f"https://spark.apache.org/docs/{version}/sql-programming-guide.html"),
        (
            "PySpark",
            f"https://spark.apache.org/docs/{version}/api/python/getting_started/index.html",
        ),
        (
            "RDD Programming Guide",
            f"https://spark.apache.org/docs/{version}/rdd-programming-guide.html",
        ),
        (
            "Structured Streaming",
            f"https://spark.apache.org/docs/{version}/streaming-programming-guide.html",
        ),
        ("MLlib", f"https://spark.apache.org/docs/{version}/ml-guide.html"),
        ("GraphX", f"https://spark.apache.org/docs/{version}/graphx-programming-guide.html"),
        ("SparkR", f"https://spark.apache.org/docs/{version}/sparkr.html"),
        (
            "Spark SQL CLI",
            f"https://spark.apache.org/docs/{version}/"
            f"sql-distributed-sql-engine-spark-sql-cli.html",
        ),
    ]
    for title, url in programming_guides:
        content.append(f"- [{title}]({url})")
    content.append("")

    content.append("## API Docs")
    content.append("")
    # TODO: Update API docs to point to their own llms.txt files once available
    # e.g., https://spark.apache.org/docs/{version}/api/python/llms.txt
    api_docs = [
        ("Spark Python API", f"https://spark.apache.org/docs/{version}/api/python/index.html"),
        (
            "Spark Scala API",
            f"https://spark.apache.org/docs/{version}/api/scala/org/apache/spark/index.html",
        ),
        ("Spark Java API", f"https://spark.apache.org/docs/{version}/api/java/index.html"),
        ("Spark R API", f"https://spark.apache.org/docs/{version}/api/R/index.html"),
        (
            "Spark SQL Built-in Functions",
            f"https://spark.apache.org/docs/{version}/api/sql/index.html",
        ),
    ]
    for title, url in api_docs:
        content.append(f"- [{title}]({url})")
    content.append("")

    content.append("## Deployment Guides")
    content.append("")
    deployment_guides = [
        ("Cluster Overview", f"https://spark.apache.org/docs/{version}/cluster-overview.html"),
        (
            "Submitting Applications",
            f"https://spark.apache.org/docs/{version}/submitting-applications.html",
        ),
        (
            "Standalone Deploy Mode",
            f"https://spark.apache.org/docs/{version}/spark-standalone.html",
        ),
        ("YARN", f"https://spark.apache.org/docs/{version}/running-on-yarn.html"),
        ("Kubernetes", f"https://spark.apache.org/docs/{version}/running-on-kubernetes.html"),
    ]
    for title, url in deployment_guides:
        content.append(f"- [{title}]({url})")
    content.append("")

    content.append("## Other Documents")
    content.append("")
    other_docs = [
        ("Configuration", f"https://spark.apache.org/docs/{version}/configuration.html"),
        ("Monitoring", f"https://spark.apache.org/docs/{version}/monitoring.html"),
        ("Web UI", f"https://spark.apache.org/docs/{version}/web-ui.html"),
        ("Tuning Guide", f"https://spark.apache.org/docs/{version}/tuning.html"),
        ("Job Scheduling", f"https://spark.apache.org/docs/{version}/job-scheduling.html"),
        ("Security", f"https://spark.apache.org/docs/{version}/security.html"),
        (
            "Hardware Provisioning",
            f"https://spark.apache.org/docs/{version}/hardware-provisioning.html",
        ),
        (
            "Cloud Infrastructures",
            f"https://spark.apache.org/docs/{version}/cloud-integration.html",
        ),
        ("Migration Guide", f"https://spark.apache.org/docs/{version}/migration-guide.html"),
    ]
    for title, url in other_docs:
        content.append(f"- [{title}]({url})")
    content.append("")

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

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(content))

    print(f"Generated {output_path}")

    total_docs = len(programming_guides) + len(api_docs) + len(deployment_guides) + len(other_docs)
    sections_count = 5

    print(f"Total documentation pages indexed: {total_docs}")
    print(f"Sections: {sections_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate llms.txt file for Apache Spark documentation"
    )
    parser.add_argument(
        "--docs-path", type=str, default="docs", help="Path to the docs directory (default: docs)"
    )
    parser.add_argument(
        "--output", type=str, default="llms.txt", help="Output file path (default: llms.txt)"
    )
    parser.add_argument(
        "--version",
        type=str,
        default="latest",
        help="Spark documentation version (default: latest)",
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

    # Generate the llms.txt file
    generate_llms_txt(docs_path, output_path, args.version)


if __name__ == "__main__":
    main()
