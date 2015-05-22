#!/usr/bin/python

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

# Audits binary and maven artifacts for a Spark release.
# Requires GPG and Maven.
# usage:
#   python audit_release.py

import os
import re
import shutil
import subprocess
import sys
import time
import urllib2

# Note: The following variables must be set before use!
RELEASE_URL = "http://people.apache.org/~andrewor14/spark-1.1.1-rc1/"
RELEASE_KEY = "XXXXXXXX" # Your 8-digit hex
RELEASE_REPOSITORY = "https://repository.apache.org/content/repositories/orgapachespark-1033"
RELEASE_VERSION = "1.1.1"
SCALA_VERSION = "2.10.4"
SCALA_BINARY_VERSION = "2.10"

# Do not set these
LOG_FILE_NAME = "spark_audit_%s" % time.strftime("%h_%m_%Y_%I_%M_%S")
LOG_FILE = open(LOG_FILE_NAME, 'w')
WORK_DIR = "/tmp/audit_%s" % int(time.time())
MAVEN_CMD = "mvn"
GPG_CMD = "gpg"
SBT_CMD = "sbt -Dsbt.log.noformat=true"

# Track failures to print them at the end
failures = []

# Log a message. Use sparingly because this flushes every write.
def log(msg):
    LOG_FILE.write(msg + "\n")
    LOG_FILE.flush()

def log_and_print(msg):
    print msg
    log(msg)

# Prompt the user to delete the scratch directory used
def clean_work_files():
    response = raw_input("OK to delete scratch directory '%s'? (y/N) " % WORK_DIR)
    if response == "y":
        shutil.rmtree(WORK_DIR)

# Run the given command and log its output to the log file
def run_cmd(cmd, exit_on_failure=True):
    log("Running command: %s" % cmd)
    ret = subprocess.call(cmd, shell=True, stdout=LOG_FILE, stderr=LOG_FILE)
    if ret != 0 and exit_on_failure:
        log_and_print("Command failed: %s" % cmd)
        clean_work_files()
        sys.exit(-1)
    return ret

def run_cmd_with_output(cmd):
    log_and_print("Running command: %s" % cmd)
    return subprocess.check_output(cmd, shell=True, stderr=LOG_FILE)

# Test if the given condition is successful
# If so, print the pass message; otherwise print the failure message
def test(cond, msg):
    return passed(msg) if cond else failed(msg)

def passed(msg):
    log_and_print("[PASSED] %s" % msg)

def failed(msg):
    failures.append(msg)
    log_and_print("[**FAILED**] %s" % msg)

def get_url(url):
    return urllib2.urlopen(url).read()

# If the path exists, prompt the user to delete it
# If the resource is not deleted, abort
def ensure_path_not_present(path):
    full_path = os.path.expanduser(path)
    if os.path.exists(full_path):
        print "Found %s locally." % full_path
        response = raw_input("This can interfere with testing published artifacts. OK to delete? (y/N) ")
        if response == "y":
            shutil.rmtree(full_path)
        else:
            print "Abort."
            sys.exit(-1)

log_and_print("|-------- Starting Spark audit tests for release %s --------|" % RELEASE_VERSION)
log_and_print("Log output can be found in %s" % LOG_FILE_NAME)

original_dir = os.getcwd()

# For each of these modules, we'll test an 'empty' application in sbt and
# maven that links against them. This will catch issues with messed up
# dependencies within those projects.
modules = [
    "spark-core", "spark-bagel", "spark-mllib", "spark-streaming", "spark-repl",
    "spark-graphx", "spark-streaming-flume", "spark-streaming-kafka",
    "spark-streaming-mqtt", "spark-streaming-twitter", "spark-streaming-zeromq",
    "spark-catalyst", "spark-sql", "spark-hive", "spark-streaming-kinesis-asl"
]
modules = map(lambda m: "%s_%s" % (m, SCALA_BINARY_VERSION), modules)

# Check for directories that might interfere with tests
local_ivy_spark = "~/.ivy2/local/org.apache.spark"
cache_ivy_spark = "~/.ivy2/cache/org.apache.spark"
local_maven_kafka = "~/.m2/repository/org/apache/kafka"
local_maven_kafka = "~/.m2/repository/org/apache/spark"
map(ensure_path_not_present, [local_ivy_spark, cache_ivy_spark, local_maven_kafka])

# SBT build tests
log_and_print("==== Building SBT modules ====")
os.chdir("blank_sbt_build")
os.environ["SPARK_VERSION"] = RELEASE_VERSION
os.environ["SCALA_VERSION"] = SCALA_VERSION
os.environ["SPARK_RELEASE_REPOSITORY"] = RELEASE_REPOSITORY
os.environ["SPARK_AUDIT_MASTER"] = "local"
for module in modules:
    log("==== Building module %s in SBT ====" % module)
    os.environ["SPARK_MODULE"] = module
    ret = run_cmd("%s clean update" % SBT_CMD, exit_on_failure=False)
    test(ret == 0, "SBT build against '%s' module" % module)
os.chdir(original_dir)

# SBT application tests
log_and_print("==== Building SBT applications ====")
for app in ["sbt_app_core", "sbt_app_graphx", "sbt_app_streaming", "sbt_app_sql", "sbt_app_hive", "sbt_app_kinesis"]:
    log("==== Building application %s in SBT ====" % app)
    os.chdir(app)
    ret = run_cmd("%s clean run" % SBT_CMD, exit_on_failure=False)
    test(ret == 0, "SBT application (%s)" % app)
    os.chdir(original_dir)

# Maven build tests
os.chdir("blank_maven_build")
log_and_print("==== Building Maven modules ====")
for module in modules:
    log("==== Building module %s in maven ====" % module)
    cmd = ('%s --update-snapshots -Dspark.release.repository="%s" -Dspark.version="%s" '
           '-Dspark.module="%s" clean compile' %
           (MAVEN_CMD, RELEASE_REPOSITORY, RELEASE_VERSION, module))
    ret = run_cmd(cmd, exit_on_failure=False)
    test(ret == 0, "maven build against '%s' module" % module)
os.chdir(original_dir)

# Maven application tests
log_and_print("==== Building Maven applications ====")
os.chdir("maven_app_core")
mvn_exec_cmd = ('%s --update-snapshots -Dspark.release.repository="%s" -Dspark.version="%s" '
                '-Dscala.binary.version="%s" clean compile '
                'exec:java -Dexec.mainClass="SimpleApp"' %
                (MAVEN_CMD, RELEASE_REPOSITORY, RELEASE_VERSION, SCALA_BINARY_VERSION))
ret = run_cmd(mvn_exec_cmd, exit_on_failure=False)
test(ret == 0, "maven application (core)")
os.chdir(original_dir)

# Binary artifact tests
if os.path.exists(WORK_DIR):
    print "Working directory '%s' already exists" % WORK_DIR
    sys.exit(-1)
os.mkdir(WORK_DIR)
os.chdir(WORK_DIR)

index_page = get_url(RELEASE_URL)
artifact_regex = r = re.compile("<a href=\"(.*.tgz)\">")
artifacts = r.findall(index_page)

# Verify artifact integrity
for artifact in artifacts:
    log_and_print("==== Verifying download integrity for artifact: %s ====" % artifact)

    artifact_url = "%s/%s" % (RELEASE_URL, artifact)
    key_file = "%s.asc" % artifact
    run_cmd("wget %s" % artifact_url)
    run_cmd("wget %s/%s" % (RELEASE_URL, key_file))
    run_cmd("wget %s%s" % (artifact_url, ".sha"))

    # Verify signature
    run_cmd("%s --keyserver pgp.mit.edu --recv-key %s" % (GPG_CMD, RELEASE_KEY))
    run_cmd("%s %s" % (GPG_CMD, key_file))
    passed("Artifact signature verified.")

    # Verify md5
    my_md5 = run_cmd_with_output("%s --print-md MD5 %s" % (GPG_CMD, artifact)).strip()
    release_md5 = get_url("%s.md5" % artifact_url).strip()
    test(my_md5 == release_md5, "Artifact MD5 verified.")

    # Verify sha
    my_sha = run_cmd_with_output("%s --print-md SHA512 %s" % (GPG_CMD, artifact)).strip()
    release_sha = get_url("%s.sha" % artifact_url).strip()
    test(my_sha == release_sha, "Artifact SHA verified.")

    # Verify Apache required files
    dir_name = artifact.replace(".tgz", "")
    run_cmd("tar xvzf %s" % artifact)
    base_files = os.listdir(dir_name)
    test("CHANGES.txt" in base_files, "Tarball contains CHANGES.txt file")
    test("NOTICE" in base_files, "Tarball contains NOTICE file")
    test("LICENSE" in base_files, "Tarball contains LICENSE file")

    os.chdir(WORK_DIR)

# Report result
log_and_print("\n")
if len(failures) == 0:
    log_and_print("*** ALL TESTS PASSED ***")
else:
    log_and_print("XXXXX SOME TESTS DID NOT PASS XXXXX")
    for f in failures:
        log_and_print("  %s" % f)
os.chdir(original_dir)

# Clean up
clean_work_files()

log_and_print("|-------- Spark release audit complete --------|")
