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

import argparse
import functools
import os
from pathlib import Path


@functools.cache
def spark_home() -> Path:
    """
    Resolve SPARK_HOME from the environment or this script's location.
    """
    env = os.environ.get("SPARK_HOME")
    if env:
        return Path(env)
    return Path(__file__).resolve().parent.parent.parent


def exec_script(args: list) -> None:
    """
    Replace the current process with the given command.

    Using exec (rather than subprocess) ensures correct signal delivery,
    natural exit-code propagation, and no intermediate process that could
    become orphaned or need to forward signals.
    """
    os.execvp(str(args[0]), [str(a) for a in args])


def register_daemon(
    subparsers: argparse._SubParsersAction,
    formatter_class,
    *,
    name: str,
    help_text: str,
    server_class: str,
    start_script: str,
    stop_script: str,
    extras=None,
) -> None:
    """
    Register a daemon command group with start/stop/status subcommands.

    Args:
        name: The subcommand name (e.g. "connect").
        help_text: Help string shown in the parent command's help.
        server_class: The JVM class name, used for spark-daemon.sh status.
        start_script: Path relative to SPARK_HOME for the start script.
        stop_script: Path relative to SPARK_HOME for the stop script.
        extras: Optional callable(sub) to register additional subcommands.
    """
    parser = subparsers.add_parser(name, help=help_text, formatter_class=formatter_class)
    sub = parser.add_subparsers(dest="subcommand", title="commands", metavar="COMMAND")

    def _start(args: argparse.Namespace, remaining: list) -> None:
        exec_script([spark_home() / start_script] + remaining)

    def _stop(args: argparse.Namespace, remaining: list) -> None:
        exec_script([spark_home() / stop_script] + remaining)

    def _status(args: argparse.Namespace, remaining: list) -> None:
        exec_script([spark_home() / "sbin" / "spark-daemon.sh", "status", server_class, "1"])

    sub.add_parser("start", help="Start the server.", add_help=False).set_defaults(func=_start)
    sub.add_parser("stop", help="Stop the server.", add_help=False).set_defaults(func=_stop)
    # spark-daemon.sh doesn't support `--help`, so we don't suppress it here.
    sub.add_parser("status", help="Show server status.").set_defaults(func=_status)

    if extras:
        extras(sub)
