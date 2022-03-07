#!/usr/bin/env python3
"""Kill a Zinc process that is listening on a given port"""
import argparse
import os
import re
import signal
import subprocess
import sys


def _parse_args():
    zinc_port_var = "ZINC_PORT"
    zinc_port_option = "--zinc-port"
    parser = argparse.ArgumentParser()
    parser.add_argument(
        zinc_port_option,
        type=int,
        default=int(os.environ.get(zinc_port_var, "0")),
        help="Specify zinc port",
    )
    args = parser.parse_args()
    if not args.zinc_port:
        parser.error(
            "Specify either environment variable {0} or option {1}".format(
                zinc_port_var, zinc_port_option
            )
        )
    return args


def _kill_processes_listening_on_port(port):
    killed = set()
    for pid in _yield_processes_listening_on_port(port):
        if not pid in killed:
            killed.add(pid)
            os.kill(pid, signal.SIGTERM)


def _yield_processes_listening_on_port(port):
    pattern = re.compile(r":{0} \(LISTEN\)".format(port))
    innocuous_errors = re.compile(
        r"^\s*Output information may be incomplete.\s*$"
        r"|^lsof: WARNING: can't stat\(\) (?:tracefs|nsfs|overlay|tmpfs|aufs|zfs) file system .*$"
        r"|^\s*$"
    )
    lsof_process = subprocess.Popen(
        ["lsof", "-P"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    stdout, stderr = lsof_process.communicate()
    if lsof_process.returncode != 0:
        raise OSError("Can't run lsof -P, stderr:\n{}".format(stderr))
    for line in stderr.split("\n"):
        if not innocuous_errors.match(line):
            sys.stderr.write(line + "\n")
    for line in stdout.split("\n"):
        if pattern.search(line):
            yield int(line.split()[1])


def _main():
    args = _parse_args()
    _kill_processes_listening_on_port(args.zinc_port)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
