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

def sanitize_url(url: str) -> str:
    """
    Sanitize a URL to remove query parameters, which may contain JWT or OAuth tokens.
    The sanitized URL is safe to be written in clear text.

    When the provided string is not a URL, it is returned with no modification.
    If the provided parameter is not a string, an AssertionError is raised.
    """
    assert type(url) is str
    from urllib.parse import urlparse

    parsed = urlparse(url)
    sanitized = f"{parsed.scheme}://" if parsed.scheme else ""
    sanitized += f"{parsed.netloc}{parsed.path}"

    return sanitized
