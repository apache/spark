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

from __future__ import print_function
import os
from test_functions import \
    identify_changed_files_from_git_commits, \
    determine_modules_for_files, \
    determine_tags_to_exclude, \
    setup_test_environ, \
    determine_modules_to_test, \
    modules


def get_build_environment():
    fields = {
        "build_tool": "sbt",
        "hadoop_version": os.environ.get("HADOOP_PROFILE", "hadooppalantir"),
        "test_env": "local"
    }
    # Make an object out of the fields
    env = type('', (object,), fields)()

    print("[info] Using build tool", env.build_tool, "with Hadoop profile", env.hadoop_version,
          "under environment", env.test_env)

    return env


def modules_to_test(env):
    changed_modules = None
    changed_files = None
    if env.test_env == "amplab_jenkins" and os.environ.get("AMP_JENKINS_PRB"):
        target_branch = os.environ["ghprbTargetBranch"]
        changed_files = identify_changed_files_from_git_commits("HEAD", target_branch=target_branch)
        changed_modules = determine_modules_for_files(changed_files)
        excluded_tags = determine_tags_to_exclude(changed_modules)
    if not changed_modules:
        changed_modules = [modules.root]
        excluded_tags = []
    print("[info] Found the following changed modules:",
          ", ".join(x.name for x in changed_modules))

    # setup environment variables
    # note - the 'root' module doesn't collect environment variables for all modules. Because the
    # environment variables should not be set if a module is not changed, even if running the 'root'
    # module. So here we should use changed_modules rather than test_modules.
    test_environ = {}
    for m in changed_modules:
        test_environ.update(m.environ)
    setup_test_environ(test_environ)

    fields = {
        'test_modules': determine_modules_to_test(changed_modules),
        'changed_files': changed_files,  # Used in run-style-checks.py
        'excluded_tags': excluded_tags
    }

    # Make an object out of the fields
    return type('', (object,), fields)()
