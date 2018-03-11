#!/usr/bin/env python

from __future__ import print_function

from build_environment import get_build_environment, modules_to_test
from test_functions import *


if __name__ == '__main__':
    # TODO(dsanduleac) - don't use the main parse_opts() from test_functions for just parallelism
    # roll out our own if we want to
    opts = parse_opts()

    env = get_build_environment()
    mtt = modules_to_test(env)

    modules_with_python_tests = [m for m in mtt.test_modules if m.python_test_goals]
    if modules_with_python_tests:
        run_python_tests(modules_with_python_tests, opts.parallelism)
        run_python_packaging_tests()