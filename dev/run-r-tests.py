#!/usr/bin/env python

from __future__ import print_function

from build_environment import get_build_environment, modules_to_test
from test_functions import *


if __name__ == '__main__':
    env = get_build_environment()
    mtt = modules_to_test(env)

    # install SparkR
    if which("R"):
        run_cmd([os.path.join(SPARK_HOME, "R", "install-dev.sh")])
    else:
        raise Exception("Cannot install SparkR as R was not found in PATH")

    if any(m.should_run_r_tests for m in mtt.test_modules):
        run_sparkr_tests()
