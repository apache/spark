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

# Use the master static imaghe by default, it will be replaced if build-args is given
ARG BASE_IMG=ghcr.io/apache/spark/apache-spark-github-action-image-cache:master-static
FROM $BASE_IMG

## Install Python linter dependencies
# TODO(SPARK-32407): Sphinx 3.1+ does not correctly index nested classes.
#   See also https://github.com/sphinx-doc/sphinx/issues/7551.
# Jinja2 3.0.0+ causes error when building with Sphinx.
#   See also https://issues.apache.org/jira/browse/SPARK-35375.
RUN python3.9 -m pip install 'flake8==3.9.0' pydata_sphinx_theme 'mypy==0.920' 'pytest-mypy-plugins==1.9.3' numpydoc 'jinja2<3.0.0' 'black==22.6.0'
RUN python3.9 -m pip install 'pandas-stubs==1.2.0.53'

## Install R linter dependencies and SparkR
RUN apt update
RUN apt-get install -y libcurl4-openssl-dev libgit2-dev libssl-dev libxml2-dev \
  libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev \
  libtiff5-dev libjpeg-dev
RUN Rscript -e "install.packages(c('devtools'), repos='https://cloud.r-project.org/')"
RUN Rscript -e "devtools::install_version('lintr', version='2.0.1', repos='https://cloud.r-project.org')"

## Instll JavaScript linter dependencies
RUN apt update
RUN apt-get install -y nodejs npm

## Install dependencies for documentation generation
# pandoc is required to generate PySpark APIs as well in nbsphinx.
RUN apt-get install -y libcurl4-openssl-dev pandoc
# TODO(SPARK-32407): Sphinx 3.1+ does not correctly index nested classes.
#   See also https://github.com/sphinx-doc/sphinx/issues/7551.
# Jinja2 3.0.0+ causes error when building with Sphinx.
#   See also https://issues.apache.org/jira/browse/SPARK-35375.
# Pin the MarkupSafe to 2.0.1 to resolve the CI error.
#   See also https://issues.apache.org/jira/browse/SPARK-38279.
RUN python3.9 -m pip install 'sphinx<3.1.0' mkdocs pydata_sphinx_theme ipython nbsphinx numpydoc 'jinja2<3.0.0' 'markupsafe==2.0.1'
RUN python3.9 -m pip install ipython_genutils # See SPARK-38517
RUN python3.9 -m pip install sphinx_plotly_directive 'numpy>=1.20.0' pyarrow pandas 'plotly>=4.8'
RUN python3.9 -m pip install 'docutils<0.18.0' # See SPARK-39421
RUN apt-get update -y
RUN apt-get install -y ruby ruby-dev
RUN Rscript -e "install.packages(c('devtools', 'testthat', 'knitr', 'rmarkdown', 'markdown', 'e1071', 'roxygen2', 'ggplot2', 'mvtnorm', 'statmod'), repos='https://cloud.r-project.org/')"
RUN Rscript -e "devtools::install_version('pkgdown', version='2.0.1', repos='https://cloud.r-project.org')"
RUN Rscript -e "devtools::install_version('preferably', version='0.4', repos='https://cloud.r-project.org')"
RUN gem install bundler
