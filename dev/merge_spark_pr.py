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

# Utility for creating well-formed pull request merges and pushing them to Apache.
#   usage: ./apache-pr-merge.py    (see config env vars below)
#
# This utility assumes you already have local a Spark git folder and that you
# have added remotes corresponding to both (i) the github apache Spark 
# mirror and (ii) the apache git repo.

import json
import os
import subprocess
import sys
import tempfile
import urllib2

# Location of your Spark git development area
SPARK_HOME = os.environ.get("SPARK_HOME", "/home/patrick/Documents/spark")
# Remote name which points to the Gihub site
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache-github")
# Remote name which points to Apache git
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache")

GIT_API_BASE = "https://api.github.com/repos/apache/incubator-spark"
# Prefix added to temporary branches
BRANCH_PREFIX = "PR_TOOL"

os.chdir(SPARK_HOME)

def get_json(url):
  try:
    return json.load(urllib2.urlopen(url))
  except urllib2.HTTPError as e:
    print "Unable to fetch URL, exiting: %s" % url
    sys.exit(-1)

def fail(msg):
  print msg
  clean_up()
  sys.exit(-1)

def run_cmd(cmd):
  if isinstance(cmd, list):
    return subprocess.check_output(cmd)
  else:
    return subprocess.check_output(cmd.split(" "))

def continue_maybe(prompt):
  result = raw_input("\n%s (y/n): " % prompt)
  if result.lower() != "y":
    fail("Okay, exiting")

original_head = run_cmd("git rev-parse HEAD")[:8]

def clean_up():
  print "Restoring head pointer to %s" % original_head
  run_cmd("git checkout %s" % original_head)

  branches = run_cmd("git branch").replace(" ", "").split("\n")

  for branch in filter(lambda x: x.startswith(BRANCH_PREFIX), branches):
    print "Deleting local branch %s" % branch
    run_cmd("git branch -D %s" % branch)

# merge the requested PR and return the merge hash
def merge_pr(pr_num, target_ref):
  pr_branch_name = "%s_MERGE_PR_%s" % (BRANCH_PREFIX, pr_num)
  target_branch_name = "%s_MERGE_PR_%s_%s" % (BRANCH_PREFIX, pr_num, target_ref.upper())
  run_cmd("git fetch %s pull/%s/head:%s" % (PR_REMOTE_NAME, pr_num, pr_branch_name))
  run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, target_ref, target_branch_name))
  run_cmd("git checkout %s" % target_branch_name)
  
  run_cmd(['git', 'merge', pr_branch_name, '--squash'])

  commit_authors = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name, 
    '--pretty=format:%an <%ae>']).split("\n")
  distinct_authors = sorted(set(commit_authors), key=lambda x: commit_authors.count(x), reverse=True)
  primary_author = distinct_authors[0]
  commits = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name, 
    '--pretty=format:%h [%an] %s']).split("\n\n")

  merge_message = "Merge pull request #%s from %s.\n\n%s\n\n%s" % (
    pr_num, pr_repo_desc, title, body)
  merge_message_parts = merge_message.split("\n\n")
  merge_message_flags = []

  for p in merge_message_parts:
    merge_message_flags = merge_message_flags + ["-m", p]
  authors = "\n".join(["Author: %s" % a for a in distinct_authors])
  merge_message_flags = merge_message_flags + ["-m", authors]
  merge_message_flags = merge_message_flags + [
    "-m", "Closes #%s and squashes the following commits:" % pr_num]
  for c in commits:
    merge_message_flags = merge_message_flags + ["-m", c]

  run_cmd(['git', 'commit', '--author="%s"' % primary_author] + merge_message_flags)

  continue_maybe("Merge complete (local ref %s). Push to %s?" % (
    target_branch_name, PUSH_REMOTE_NAME))

  try:
    run_cmd('git push %s %s:%s' % (PUSH_REMOTE_NAME, target_branch_name, target_ref))
  except Exception as e:
    clean_up()
    fail("Exception while pushing: %s" % e)
  
  merge_hash = run_cmd("git rev-parse %s" % target_branch_name)[:8]
  clean_up()
  print("Pull request #%s merged!" % pr_num)
  print("Merge hash: %s" % merge_hash)
  return merge_hash


def maybe_cherry_pick(pr_num, merge_hash, default_branch):
  continue_maybe("Would you like to pick %s into another branch?" % merge_hash)
  pick_ref = raw_input("Enter a branch name [%s]: " % default_branch)
  if pick_ref == "":
    pick_ref = default_branch

  pick_branch_name = "%s_PICK_PR_%s_%s" % (BRANCH_PREFIX, pr_num, pick_ref.upper())

  run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, pick_ref, pick_branch_name))
  run_cmd("git checkout %s" % pick_branch_name)
  run_cmd("git cherry-pick -sx %s" % merge_hash)
  
  continue_maybe("Pick complete (local ref %s). Push to %s?" % (
    pick_branch_name, PUSH_REMOTE_NAME))

  try:
    run_cmd('git push %s %s:%s' % (PUSH_REMOTE_NAME, pick_branch_name, pick_ref))
  except Exception as e:
    clean_up()
    fail("Exception while pushing: %s" % e)

  pick_hash = run_cmd("git rev-parse %s" % pick_branch_name)[:8]
  clean_up()

  print("Pull request #%s picked into %s!" % (pr_num, pick_ref))
  print("Pick hash: %s" % pick_hash)

branches = get_json("%s/branches" % GIT_API_BASE)
branch_names = filter(lambda x: x.startswith("branch-"), [x['name'] for x in branches])
# Assumes branch names can be sorted lexicographically
latest_branch = sorted(branch_names, reverse=True)[0]

pr_num = raw_input("Which pull request would you like to merge? (e.g. 34): ")
pr = get_json("%s/pulls/%s" % (GIT_API_BASE, pr_num))

url = pr["url"]
title = pr["title"]
body = pr["body"]
target_ref = pr["base"]["ref"]
user_login = pr["user"]["login"]
base_ref = pr["head"]["ref"]
pr_repo_desc = "%s/%s" % (user_login, base_ref)

if pr["merged"] == True:
  print "Pull request %s has already been merged, assuming you want to backport" % pr_num
  merge_commit_desc = run_cmd(['git', 'log', '--merges', '--first-parent', 
    '--grep=pull request #%s' % pr_num, '--oneline']).split("\n")[0]
  if merge_commit_desc == "":
    fail("Couldn't find any merge commit for #%s, you may need to update HEAD." % pr_num)

  merge_hash = merge_commit_desc[:7]  
  message = merge_commit_desc[8:]
  
  print "Found: %s" % message
  maybe_cherry_pick(pr_num, merge_hash, latest_branch)
  sys.exit(0)

if bool(pr["mergeable"]) == False:
  fail("Pull request %s is not mergeable in its current form" % pr_num)

print ("\n=== Pull Request #%s ===" % pr_num)
print("title\t%s\nsource\t%s\ntarget\t%s\nurl\t%s" % (
  title, pr_repo_desc, target_ref, url))
continue_maybe("Proceed with merging pull request #%s?" % pr_num)

merge_hash = merge_pr(pr_num, target_ref)

while True:
  maybe_cherry_pick(pr_num, merge_hash, latest_branch)
