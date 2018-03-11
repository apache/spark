#!/usr/bin/env python

from __future__ import print_function

from build_environment import get_build_environment
from test_functions import *


if __name__ == '__main__':
    env = get_build_environment()

    # ensure we have run the spark build
    build_apache_spark(env.build_tool, env.hadoop_version)

    # backwards compatibility checks
    if env.build_tool == "sbt":
        # Note: compatibility tests only supported in sbt for now
        detect_binary_inop_with_mima(env.hadoop_version)