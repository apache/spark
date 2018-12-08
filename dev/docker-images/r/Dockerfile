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

FROM palantirtechnologies/circle-spark-base

USER root

### Install R
RUN apt-get update \
  && apt-get install r-base r-base-dev qpdf \
  && rm -rf /var/lib/apt/lists/* \
  && chmod 777 /usr/local/lib/R/site-library \
  && /usr/lib/R/bin/R -e "install.packages(c('devtools'), repos='http://cran.us.r-project.org', lib='/usr/local/lib/R/site-library'); devtools::install_github('r-lib/testthat@v2.0.0', lib='/usr/local/lib/R/site-library'); install.packages(c('knitr', 'rmarkdown', 'e1071', 'survival', 'roxygen2', 'lintr'), repos='http://cran.us.r-project.org', lib='/usr/local/lib/R/site-library')"
ENV R_HOME=/usr/lib/R

USER circleci
