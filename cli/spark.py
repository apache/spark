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

"""
Unified CLI entry point for Apache Spark.

This script dispatches subcommands to the appropriate underlying scripts
under bin/ and sbin/. It requires only the Python standard library and
targets Python 3.11+.
"""

import argparse
import os
import sys

CLI_HOME = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, CLI_HOME)

from _spark_cli.utils import exec_script, register_daemon, spark_home


class _HelpFormatter(argparse.HelpFormatter):
    """
    Custom formatter that hides the metavar heading inside subparser groups.
    """

    def _format_action(self, action: argparse.Action) -> str:
        # For subparser choice groups, skip the top-level metavar line.
        if isinstance(action, argparse._SubParsersAction):
            parts = []
            for choice_action in action._get_subactions():
                parts.append(self._format_action(choice_action))
            return "".join(parts)
        return super()._format_action(action)


class _GroupedArgumentParser(argparse.ArgumentParser):
    """
    Top-level parser that renders subcommands in named groups.

    argparse unfortunately does not support this out of the box.

    See: https://github.com/python/cpython/issues/53587
    """

    def format_help(self) -> str:
        formatter = self._get_formatter()
        formatter.add_usage(self.usage, self._actions, self._mutually_exclusive_groups)
        formatter.add_text(self.description)

        # Render non-subparser action groups (typically just "options").
        for action_group in self._action_groups:
            if any(
                isinstance(a, argparse._SubParsersAction)
                for a in action_group._group_actions
            ):
                continue
            formatter.start_section(action_group.title)
            formatter.add_arguments(action_group._group_actions)
            formatter.end_section()

        subparsers_action = None
        for action in self._actions:
            if isinstance(action, argparse._SubParsersAction):
                subparsers_action = action
                break

        if subparsers_action:
            groups = getattr(subparsers_action, "_command_groups", [])
            actions_by_name = {
                ca.dest: ca for ca in subparsers_action._get_subactions()
            }
            rendered: set = set()
            for title, names in groups:
                formatter.start_section(title)
                for name in names:
                    if name in actions_by_name:
                        formatter.add_argument(actions_by_name[name])
                        rendered.add(name)
                formatter.end_section()

            ungrouped = [
                ca
                for ca in subparsers_action._get_subactions()
                if ca.dest not in rendered
            ]
            if ungrouped:
                formatter.start_section("commands")
                for ca in ungrouped:
                    formatter.add_argument(ca)
                formatter.end_section()

        formatter.add_text(self.epilog)
        return formatter.format_help()


def _register_worker(subparsers, formatter_class) -> None:
    """
    Register the 'worker' command and its subcommands.
    """

    def _start(args, remaining):
        exec_script([spark_home() / "sbin" / "start-worker.sh"] + remaining)

    def _start_all(args, remaining):
        exec_script([spark_home() / "sbin" / "start-workers.sh"] + remaining)

    def _stop(args, remaining):
        exec_script([spark_home() / "sbin" / "stop-worker.sh"] + remaining)

    def _stop_all(args, remaining):
        exec_script([spark_home() / "sbin" / "stop-workers.sh"] + remaining)

    def _status(args, remaining):
        exec_script([
            spark_home() / "sbin" / "spark-daemon.sh",
            "status", "org.apache.spark.deploy.worker.Worker", "1",
        ])

    def _decommission(args, remaining):
        exec_script([spark_home() / "sbin" / "decommission-worker.sh"] + remaining)

    def _run(args, remaining):
        exec_script([spark_home() / "sbin" / "workers.sh"] + remaining)

    parser = subparsers.add_parser(
        "worker", help="Manage the standalone cluster workers.",
        formatter_class=formatter_class,
    )
    sub = parser.add_subparsers(dest="subcommand", title="commands", metavar="COMMAND")
    sub.add_parser(
        "start", help="Start a worker.", add_help=False
    ).set_defaults(func=_start)
    sub.add_parser(
        "start-all", help="Start all workers via SSH.", add_help=False
    ).set_defaults(func=_start_all)
    sub.add_parser(
        "stop", help="Stop a worker.", add_help=False
    ).set_defaults(func=_stop)
    sub.add_parser(
        "stop-all", help="Stop all workers via SSH.", add_help=False
    ).set_defaults(func=_stop_all)
    sub.add_parser(
        "status", help="Show worker status."
    ).set_defaults(func=_status)
    sub.add_parser(
        "decommission", help="Decommission a worker.", add_help=False
    ).set_defaults(func=_decommission)
    sub.add_parser(
        "run", help="Run a command on all worker hosts via SSH.", add_help=False
    ).set_defaults(func=_run)


def _register_cluster(subparsers, formatter_class) -> None:
    """
    Register the 'cluster' command and its subcommands.
    """

    def _start(args, remaining):
        exec_script([spark_home() / "sbin" / "start-all.sh"] + remaining)

    def _stop(args, remaining):
        exec_script([spark_home() / "sbin" / "stop-all.sh"] + remaining)

    parser = subparsers.add_parser(
        "cluster", help="Manage the standalone cluster (master + workers).",
        formatter_class=formatter_class,
    )
    sub = parser.add_subparsers(dest="subcommand", title="commands", metavar="COMMAND")
    sub.add_parser(
        "start", help="Start the master and all workers.", add_help=False
    ).set_defaults(func=_start)
    sub.add_parser(
        "stop", help="Stop the master and all workers.", add_help=False
    ).set_defaults(func=_stop)


def _register_passthrough(subparsers, name, help_text, script) -> None:
    """
    Register a passthrough command that forwards to an underlying script.
    """

    def _run(args, remaining):
        exec_script([spark_home() / script] + remaining)

    subparsers.add_parser(
        name, help=help_text, add_help=False
    ).set_defaults(func=_run)


def _build_parser() -> argparse.ArgumentParser:
    parser = _GroupedArgumentParser(
        prog="spark",
        description="Unified CLI for Apache Spark.",
        formatter_class=_HelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers._command_groups = [
        ("standalone cluster commands", ["cluster", "master", "worker"]),
        ("shells", ["scala", "python", "sql"]),
    ]

    register_daemon(
        subparsers, _HelpFormatter,
        name="connect",
        help_text="Manage the Connect server.",
        server_class="org.apache.spark.sql.connect.service.SparkConnectServer",
        start_script="sbin/start-connect-server.sh",
        stop_script="sbin/stop-connect-server.sh",
    )
    register_daemon(
        subparsers, _HelpFormatter,
        name="history",
        help_text="Manage the History server.",
        server_class="org.apache.spark.deploy.history.HistoryServer",
        start_script="sbin/start-history-server.sh",
        stop_script="sbin/stop-history-server.sh",
    )
    register_daemon(
        subparsers, _HelpFormatter,
        name="master",
        help_text="Manage the standalone cluster master.",
        server_class="org.apache.spark.deploy.master.Master",
        start_script="sbin/start-master.sh",
        stop_script="sbin/stop-master.sh",
    )
    register_daemon(
        subparsers, _HelpFormatter,
        name="thrift",
        help_text="Manage the Thrift server.",
        server_class="org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
        start_script="sbin/start-thriftserver.sh",
        stop_script="sbin/stop-thriftserver.sh",
    )

    _register_worker(subparsers, _HelpFormatter)
    _register_cluster(subparsers, _HelpFormatter)

    _register_passthrough(subparsers, "pipelines", "Run Spark Declarative Pipelines.", "bin/spark-pipelines")
    _register_passthrough(subparsers, "python", "Start the PySpark shell.", "bin/pyspark")
    _register_passthrough(subparsers, "scala", "Start the Scala shell.", "bin/spark-shell")
    _register_passthrough(subparsers, "sql", "Start the SQL shell.", "bin/spark-sql")
    _register_passthrough(subparsers, "submit", "Submit an application.", "bin/spark-submit")

    return parser


def main() -> None:
    parser = _build_parser()
    args, remaining = parser.parse_known_args()

    if not args.command:
        parser.print_help(sys.stderr)
        raise SystemExit(0)

    if not hasattr(args, "func"):
        # Command given but no subcommand (e.g. "spark connect" with no action).
        # Re-parse to get the sub-parser and print its help.
        parser.parse_args([args.command, "--help"])
        raise SystemExit(0)

    args.func(args, remaining)


if __name__ == "__main__":
    main()
