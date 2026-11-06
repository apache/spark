#!/usr/bin/env bash

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

# Dispatches a workflow on a branch for the branch CI schedulers
# (.github/workflows/branch*_scheduler.yml).
#
# Usage: dispatch-if-changed.sh <workflow file> <branch>
#
# On a scheduled run, the dispatch is skipped if the branch head is the commit of the last
# successful run of the workflow on the branch. Otherwise, including when there is no
# successful run or a lookup fails, the workflow is dispatched.
# GH_TOKEN needs the 'actions: write' and 'contents: read' permissions.

WORKFLOW="$1"
BRANCH="$2"

if [[ "$GITHUB_EVENT_NAME" == "schedule" ]]; then
  HEAD_SHA=$(gh api "repos/$GITHUB_REPOSITORY/git/ref/heads/$BRANCH" --jq .object.sha)
  LAST_SHA=$(gh run list --repo "$GITHUB_REPOSITORY" --workflow "$WORKFLOW" --branch "$BRANCH" \
    --status success --limit 1 --json headSha --jq '.[0].headSha')
  if [[ "$HEAD_SHA" =~ ^[0-9a-f]{40}$ && "$HEAD_SHA" == "$LAST_SHA" ]]; then
    MESSAGE="Skipped $WORKFLOW on $BRANCH: unchanged since the last successful run at $HEAD_SHA."
    echo "::notice::$MESSAGE"
    echo "$MESSAGE" >> "$GITHUB_STEP_SUMMARY"
    exit 0
  fi
fi

gh workflow run "$WORKFLOW" --repo "$GITHUB_REPOSITORY" --ref "$BRANCH"
