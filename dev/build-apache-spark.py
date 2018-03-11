#!/usr/bin/env python

from test_functions import *
from build_environment import get_build_environment

if __name__ == '__main__':
    env = get_build_environment()

    build_apache_spark(env.build_tool, env.hadoop_version)
    
    if env.build_tool == "sbt":
        # TODO(dsanduleac): since this is required for tests, might as well run it right
        # away in our build step.

        # Since we did not build assembly/package before running dev/mima, we need to
        # do it here because the tests still rely on it; see SPARK-13294 for details.
        build_spark_assembly_sbt(env.hadoop_version)
