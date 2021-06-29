#!/usr/bin/env python3

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
# Utility for updating JIRA's with information about GitHub pull requests

import json
import os
import re
import sys
from urllib.request import urlopen
from urllib.request import Request
from urllib.error import HTTPError

try:
    import jira.client
except ImportError:
    print("This tool requires the jira-python library")
    print("Install using 'sudo pip3 install jira'")
    sys.exit(-1)

# User facing configs
GITHUB_API_BASE = os.environ.get("GITHUB_API_BASE", "https://api.github.com/repos/apache/spark")
GITHUB_OAUTH_KEY = os.environ.get("GITHUB_OAUTH_KEY")
JIRA_PROJECT_NAME = os.environ.get("JIRA_PROJECT_NAME", "SPARK")
JIRA_API_BASE = os.environ.get("JIRA_API_BASE", "https://issues.apache.org/jira")
JIRA_USERNAME = os.environ.get("JIRA_USERNAME", "apachespark")
JIRA_PASSWORD = os.environ.get("JIRA_PASSWORD", "XXX")
# Maximum number of updates to perform in one run
MAX_UPDATES = int(os.environ.get("MAX_UPDATES", "100000"))
# Cut-off for oldest PR on which to comment. Useful for avoiding
# "notification overload" when running for the first time.
MIN_COMMENT_PR = int(os.environ.get("MIN_COMMENT_PR", "1496"))

# File used as an optimization to store maximum previously seen PR
# Used mostly because accessing ASF JIRA is slow, so we want to avoid checking
# the state of JIRA's that are tied to PR's we've already looked at.
MAX_FILE = ".github-jira-max"


def get_url(url):
    try:
        request = Request(url)
        request.add_header('Authorization', 'token %s' % GITHUB_OAUTH_KEY)
        return urlopen(request)
    except HTTPError:
        print("Unable to fetch URL, exiting: %s" % url)
        sys.exit(-1)


def get_json(urllib_response):
    return json.loads(urllib_response.read().decode("utf-8"))


# Return a list of (JIRA id, JSON dict) tuples:
# e.g. [('SPARK-1234', {.. json ..}), ('SPARK-5687', {.. json ..})}
def get_jira_prs():
    result = []
    has_next_page = True
    page_num = 0
    while has_next_page:
        page = get_url(GITHUB_API_BASE + "/pulls?page=%s&per_page=100" % page_num)
        page_json = get_json(page)

        for pull in page_json:
            jiras = re.findall(JIRA_PROJECT_NAME + "-[0-9]{4,5}", pull['title'])
            for jira in jiras:
                result = result + [(jira, pull)]

        # Check if there is another page
        link_headers = list(filter(lambda k: k.startswith("Link"), page.headers))
        if not link_headers or "next" not in link_headers[0]:
            has_next_page = False
        else:
            page_num += 1
    return result


def set_max_pr(max_val):
    f = open(MAX_FILE, 'w')
    f.write("%s" % max_val)
    f.close()
    print("Writing largest PR number seen: %s" % max_val)


def get_max_pr():
    if os.path.exists(MAX_FILE):
        result = int(open(MAX_FILE, 'r').read())
        print("Read largest PR number previously seen: %s" % result)
        return result
    else:
        return 0


def build_pr_component_dic(jira_prs):
    print("Build PR dictionary")
    dic = {}
    for issue, pr in jira_prs:
        print(issue)
        page = get_json(get_url(JIRA_API_BASE + "/rest/api/2/issue/" + issue))
        jira_components = [c['name'].upper() for c in page['fields']['components']]
        if pr['number'] in dic:
            dic[pr['number']][1].update(jira_components)
        else:
            pr_components = set(label['name'].upper() for label in pr['labels'])
            dic[pr['number']] = (pr_components, set(jira_components))
    return dic


def reset_pr_labels(pr_num, jira_components):
    url = '%s/issues/%s/labels' % (GITHUB_API_BASE, pr_num)
    labels = ', '.join(('"%s"' % c) for c in jira_components)
    try:
        request = Request(url, data=('{"labels":[%s]}' % labels).encode('utf-8'))
        request.add_header('Content-Type', 'application/json')
        request.add_header('Authorization', 'token %s' % GITHUB_OAUTH_KEY)
        request.get_method = lambda: 'PUT'
        urlopen(request)
        print("Set %s with labels %s" % (pr_num, labels))
    except HTTPError:
        print("Unable to update PR labels, exiting: %s" % url)
        sys.exit(-1)


jira_client = jira.client.JIRA({'server': JIRA_API_BASE},
                               basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))

jira_prs = get_jira_prs()

previous_max = get_max_pr()
print("Retrieved %s JIRA PR's from GitHub" % len(jira_prs))
jira_prs = [(k, v) for k, v in jira_prs if int(v['number']) > previous_max]
print("%s PR's remain after excluding visited ones" % len(jira_prs))

num_updates = 0
considered = []
for issue, pr in sorted(jira_prs, key=lambda kv: int(kv[1]['number'])):
    if num_updates >= MAX_UPDATES:
        break
    pr_num = int(pr['number'])

    print("Checking issue %s" % issue)
    considered = considered + [pr_num]

    url = pr['html_url']
    title = "[GitHub] Pull Request #%s (%s)" % (pr['number'], pr['user']['login'])
    try:
        page = get_json(get_url(JIRA_API_BASE + "/rest/api/2/issue/" + issue + "/remotelink"))
        existing_links = map(lambda l: l['object']['url'], page)
    except:
        print("Failure reading JIRA %s (does it exist?)" % issue)
        print(sys.exc_info()[0])
        continue

    if url in existing_links:
        continue

    icon = {"title": "Pull request #%s" % pr['number'],
            "url16x16": "https://assets-cdn.github.com/favicon.ico"}
    destination = {"title": title, "url": url, "icon": icon}
    # For all possible fields see:
    # https://developer.atlassian.com/display/JIRADEV/Fields+in+Remote+Issue+Links
    # application = {"name": "GitHub pull requests", "type": "org.apache.spark.jira.github"}
    jira_client.add_remote_link(issue, destination)

    comment = "User '%s' has created a pull request for this issue:" % pr['user']['login']
    comment += "\n%s" % pr['html_url']
    if pr_num >= MIN_COMMENT_PR:
        jira_client.add_comment(issue, comment)

    print("Added link %s <-> PR #%s" % (issue, pr['number']))
    num_updates += 1

if len(considered) > 0:
    set_max_pr(max(considered))


# Additionally, expose the JIRA labels to the PR
num_updates = 0
for pr_num, (pr_components, jira_components) in build_pr_component_dic(jira_prs).items():
    print(pr_num)
    if pr_components == jira_components:
        continue
    if num_updates >= MAX_UPDATES:
        break
    reset_pr_labels(pr_num, jira_components)
    num_updates += 1
