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

# Image for building Spark releases. Based on Ubuntu 20.04.
#
# Includes:
# * Java 8
# * Ivy
# * Python (3.8.5)
# * R-base/R-base-dev (4.0.3)
# * Ruby (2.7.0)
#
# You can test it as below:
#   cd dev/create-release/spark-rm
#   docker build -t spark-rm --build-arg UID=$UID .

FROM ubuntu:20.04

# For apt to be noninteractive
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

# These arguments are just for reuse and not really meant to be customized.
ARG APT_INSTALL="apt-get install --no-install-recommends -y"

# TODO(SPARK-32407): Sphinx 3.1+ does not correctly index nested classes.
#   See also https://github.com/sphinx-doc/sphinx/issues/7551.
#   We should use the latest Sphinx version once this is fixed.
# TODO(SPARK-35375): Jinja2 3.0.0+ causes error when building with Sphinx.
#   See also https://issues.apache.org/jira/browse/SPARK-35375.
ARG PIP_PKGS="sphinx==3.0.4 mkdocs==1.1.2 numpy==1.19.4 pydata_sphinx_theme==0.4.1 ipython==7.19.0 nbsphinx==0.8.0 numpydoc==1.1.0 jinja2==2.11.3 twine==3.4.1 sphinx-plotly-directive==0.1.3"
ARG GEM_PKGS="bundler:2.2.9"

# Install extra needed repos and refresh.
# - CRAN repo
# - Ruby repo (for doc generation)
#
# This is all in a single "RUN" command so that if anything changes, "apt update" is run to fetch
# the most current package versions (instead of potentially using old versions cached by docker).
RUN apt-get clean && apt-get update && $APT_INSTALL gnupg ca-certificates && \
  echo 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/' >> /etc/apt/sources.list && \
  gpg --keyserver keyserver.ubuntu.com --recv-key E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
  gpg -a --export E084DAB9 | apt-key add - && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  apt-get clean && \
  apt-get update && \
  $APT_INSTALL software-properties-common && \
  apt-get update && \
  # Install openjdk 8.
  $APT_INSTALL openjdk-8-jdk && \
  update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java && \
  # Install build / source control tools
  $APT_INSTALL curl wget git maven ivy subversion make gcc lsof libffi-dev \
    pandoc pandoc-citeproc libssl-dev libcurl4-openssl-dev libxml2-dev && \
  curl -sL https://deb.nodesource.com/setup_12.x | bash && \
  $APT_INSTALL nodejs && \
  # Install needed python packages. Use pip for installing packages (for consistency).
  $APT_INSTALL python-is-python3 python3-pip python3-setuptools && \
  # qpdf is required for CRAN checks to pass.
  $APT_INSTALL qpdf jq && \
  pip3 install $PIP_PKGS && \
  # Install R packages and dependencies used when building.
  # R depends on pandoc*, libssl (which are installed above).
  # Note that PySpark doc generation also needs pandoc due to nbsphinx
  $APT_INSTALL r-base r-base-dev && \
  $APT_INSTALL libcurl4-openssl-dev libgit2-dev libssl-dev libxml2-dev && \
  $APT_INSTALL texlive-latex-base texlive texlive-fonts-extra texinfo qpdf && \
  Rscript -e "install.packages(c('curl', 'xml2', 'httr', 'devtools', 'testthat', 'knitr', 'rmarkdown', 'roxygen2', 'e1071', 'survival'), repos='https://cloud.r-project.org/')" && \
  Rscript -e "devtools::install_github('jimhester/lintr')" && \
  # Install tools needed to build the documentation.
  $APT_INSTALL ruby2.7 ruby2.7-dev && \
  gem install --no-document $GEM_PKGS

WORKDIR /opt/spark-rm/output

ARG UID
RUN useradd -m -s /bin/bash -p spark-rm -u $UID spark-rm
USER spark-rm:spark-rm

ENTRYPOINT [ "/opt/spark-rm/do-release.sh" ]
