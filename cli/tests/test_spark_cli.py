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

import subprocess
import unittest
from pathlib import Path

CLI_HOME = Path(__file__).resolve().parent.parent
SPARK_HOME = CLI_HOME.parent
CLI = SPARK_HOME / "bin" / "spark"


def run_cli(*args):
    return subprocess.run(
        [str(CLI), *args],
        capture_output=True,
        text=True,
    )


class SparkCLITests(unittest.TestCase):
    def test_no_args_shows_help(self):
        result = run_cli()
        assert result.returncode == 0
        assert "Unified CLI for Apache Spark." in result.stderr

    def test_help_flag(self):
        result = run_cli("--help")
        assert result.returncode == 0
        assert "standalone cluster" in result.stdout

    def test_all_top_level_commands_listed(self):
        result = run_cli("--help")
        for cmd in [
            "cluster", "master", "worker",
            "scala", "python", "sql",
            "connect", "history", "thrift", "pipelines", "submit",
        ]:
            assert cmd in result.stdout, f"{cmd!r} missing from help output"

    def test_subcommand_no_args_shows_help(self):
        result = run_cli("connect")
        assert result.returncode == 0
        assert "start" in result.stdout
        assert "stop" in result.stdout
        assert "status" in result.stdout

    def test_worker_subcommands(self):
        result = run_cli("worker", "--help")
        assert result.returncode == 0
        for sub in ["start", "start-all", "stop", "stop-all", "status", "decommission", "run"]:
            assert sub in result.stdout, f"{sub!r} missing from worker help"

    def test_unknown_command(self):
        result = run_cli("bogus")
        assert result.returncode != 0


if __name__ == "__main__":
    unittest.main()
