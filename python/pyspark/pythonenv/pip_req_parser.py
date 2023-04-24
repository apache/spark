# -*- coding: utf-8 -*-
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

import json
import sys
import subprocess
from threading import Timer
import tempfile
import os
import pkg_resources
import importlib_metadata
from itertools import filterfalse, chain
from collections import namedtuple
import logging
import re
from typing import NamedTuple, Optional
from pathlib import Path


# Represents a pip requirement.
#
# :param req_str: A requirement string (e.g. "scikit-learn == 0.24.2").
# :param is_constraint: A boolean indicating whether this requirement is a constraint.
_Requirement = namedtuple("_Requirement", ["req_str", "is_constraint"])


def _is_comment(line):
    return line.startswith("#")


def _is_empty(line):
    return line == ""


def _strip_inline_comment(line):
    return line[: line.find(" #")].rstrip() if " #" in line else line


def _is_requirements_file(line):
    return line.startswith("-r ") or line.startswith("--requirement ")


def _is_constraints_file(line):
    return line.startswith("-c ") or line.startswith("--constraint ")


def _join_continued_lines(lines):
    """
    Joins lines ending with '\\'.
    >>> _join_continued_lines["a\\", "b\\", "c"]
    >>> 'abc'
    """
    continued_lines = []

    for line in lines:
        if line.endswith("\\"):
            continued_lines.append(line.rstrip("\\"))
        else:
            continued_lines.append(line)
            yield "".join(continued_lines)
            continued_lines.clear()

    # The last line ends with '\'
    if continued_lines:
        yield "".join(continued_lines)


def _parse_requirements(requirements, is_constraint, base_dir=None):
    """
    A simplified version of `pip._internal.req.parse_requirements` which performs the following
    operations on the given requirements file and yields the parsed requirements.
    - Remove comments and blank lines
    - Join continued lines
    - Resolve requirements file references (e.g. '-r requirements.txt')
    - Resolve constraints file references (e.g. '-c constraints.txt')
    :param requirements: A string path to a requirements file on the local filesystem or
                         an iterable of pip requirement strings.
    :param is_constraint: Indicates the parsed requirements file is a constraint file.
    :param base_dir: If specified, resolve relative file references (e.g. '-r requirements.txt')
                     against the specified directory.
    :return: A list of ``_Requirement`` instances.
    References:
    - `pip._internal.req.parse_requirements`:
      https://github.com/pypa/pip/blob/7a77484a492c8f1e1f5ef24eaf71a43df9ea47eb/src/pip/_internal/req/req_file.py#L118
    - Requirements File Format:
      https://pip.pypa.io/en/stable/cli/pip_install/#requirements-file-format
    - Constraints Files:
      https://pip.pypa.io/en/stable/user_guide/#constraints-files
    """
    if base_dir is None:
        if isinstance(requirements, (str, Path)):
            base_dir = os.path.dirname(requirements)
            with open(requirements) as f:
                requirements = f.read().splitlines()
        else:
            base_dir = os.getcwd()

    lines = map(str.strip, requirements)
    lines = map(_strip_inline_comment, lines)
    lines = _join_continued_lines(lines)
    lines = filterfalse(_is_comment, lines)
    lines = filterfalse(_is_empty, lines)

    for line in lines:
        if _is_requirements_file(line):
            req_file = line.split(maxsplit=1)[1]
            # If `req_file` is an absolute path, `os.path.join` returns `req_file`:
            # https://docs.python.org/3/library/os.path.html#os.path.join
            abs_path = os.path.join(base_dir, req_file)
            yield from _parse_requirements(abs_path, is_constraint=False)
        elif _is_constraints_file(line):
            req_file = line.split(maxsplit=1)[1]
            abs_path = os.path.join(base_dir, req_file)
            yield from _parse_requirements(abs_path, is_constraint=True)
        else:
            yield _Requirement(line, is_constraint)


def parse_pip_requirements(pip_requirements):
    """
    Parses an iterable of pip requirement strings or a pip requirements file.
    :param pip_requirements: Either an iterable of pip requirement strings
        (e.g. ``["scikit-learn", "-r requirements.txt"]``) or the string path to a pip requirements
        file on the local filesystem (e.g. ``"requirements.txt"``). If ``None``, an empty list will
        be returned.
    :return: A tuple of parsed requirements and constraints.
    """
    if pip_requirements is None:
        return [], []

    def _is_string(x):
        return isinstance(x, str)

    def _is_iterable(x):
        try:
            iter(x)
            return True
        except Exception:
            return False

    if _is_string(pip_requirements):
        with open(pip_requirements) as f:
            return parse_pip_requirements(f.read().splitlines())
    elif _is_iterable(pip_requirements) and all(map(_is_string, pip_requirements)):
        requirements = []
        constraints = []
        for req_or_con in _parse_requirements(pip_requirements, is_constraint=False):
            if req_or_con.is_constraint:
                constraints.append(req_or_con.req_str)
            else:
                requirements.append(req_or_con.req_str)

        return requirements, constraints
    else:
        raise TypeError(
            "`pip_requirements` must be either a string path to a pip requirements file on the "
            "local filesystem or an iterable of pip requirement strings, but got `{}`".format(
                type(pip_requirements)
            )
        )
