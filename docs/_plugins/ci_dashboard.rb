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

# Generate after Jekyll copies the static dashboard and its assets. Keep generated
# workflow metadata out of source control and reuse the existing Pages deployment.
Jekyll::Hooks.register :site, :post_write do |site|
  root = File.expand_path('..', site.source)
  generator = File.join(root, 'dev', 'generate-ci-dashboard.py')
  output = File.join(site.dest, 'ci-dashboard-data.json')
  args = ['python3', generator, '--repo', root, '--output', output]
  if ENV['CI_DASHBOARD_HISTORY'] == '1'
    args += ['--history', '--history-cache',
             File.join(root, 'docs', '_generated', 'ci-dashboard-history.json')]
  end
  unless system(*args)
    raise 'Failed to generate CI dashboard metadata'
  end
end
