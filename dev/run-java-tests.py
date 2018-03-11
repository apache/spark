#!/usr/bin/env python

from __future__ import print_function

from build_environment import get_build_environment, modules_to_test
from test_functions import *


def java_version():
    java_exe = determine_java_executable()
    if not java_exe:
        raise Exception("Cannot find a version of `java` on the system; please" \
              + " install one and retry.")
    return determine_java_version(java_exe)


if __name__ == '__main__':
    print("Found java version: {}".format(java_version()))

    env = get_build_environment()
    mtt = modules_to_test(env)

    # run the test suites
    run_scala_tests(env.build_tool, env.hadoop_version, mtt.test_modules, mtt.excluded_tags)