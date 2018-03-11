#!/usr/bin/env python

from __future__ import print_function

from test_functions import *
from build_environment import get_build_environment, modules_to_test


if __name__ == '__main__':
    env = get_build_environment()
    mtt = modules_to_test(env)
    changed_files = mtt.changed_files

    # license checks
    run_apache_rat_checks()

    # style checks
    if not changed_files or any(f.endswith(".scala")
                                or f.endswith("scalastyle-config.xml")
                                for f in changed_files):
        run_scala_style_checks()
    if not changed_files or any(f.endswith(".java")
                                or f.endswith("checkstyle.xml")
                                or f.endswith("checkstyle-suppressions.xml")
                                for f in changed_files):
        run_java_style_checks()
        pass
    if not changed_files or any(f.endswith("lint-python")
                                or f.endswith("tox.ini")
                                or f.endswith(".py")
                                for f in changed_files):
        run_python_style_checks()
    if not changed_files or any(f.endswith(".R")
                                or f.endswith("lint-r")
                                or f.endswith(".lintr")
                                for f in changed_files):
        run_sparkr_style_checks()
