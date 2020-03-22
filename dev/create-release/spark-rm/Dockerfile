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

# Image for building Spark releases. Based on Ubuntu 18.04.
#
# Includes:
# * Java 8
# * Ivy
# * Python 3.7
# * Ruby 2.7
# * R-base/R-base-dev (3.6.1)

FROM ubuntu:18.04

# For apt to be noninteractive
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

# These arguments are just for reuse and not really meant to be customized.
ARG APT_INSTALL="apt-get install --no-install-recommends -y"

ARG PIP_PKGS="sphinx==2.3.1 mkdocs==1.0.4 numpy==1.18.1"
ARG GEM_PKGS="jekyll:4.0.0 jekyll-redirect-from:0.16.0 rouge:3.15.0"

# Install extra needed repos and refresh.
# - CRAN repo
RUN apt-get clean && apt-get update && $APT_INSTALL gnupg ca-certificates && \
  echo 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' >> /etc/apt/sources.list && \
  gpg --keyserver keyserver.ubuntu.com --recv-key E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
  gpg -a --export E084DAB9 | apt-key add - && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  apt-get clean && \
  apt-get update && \
  # Install openjdk 8.
  $APT_INSTALL openjdk-8-jdk && \
  update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java && \
  # Install build / source control tools
  $APT_INSTALL curl wget git maven ivy subversion make gcc lsof libffi-dev \
    pandoc pandoc-citeproc libssl-dev libcurl4-openssl-dev libxml2-dev

ENV PATH "$PATH:/root/.pyenv/bin:/root/.pyenv/shims"
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/dd3f7d0914c5b4a416ca71ffabdf2954f2021596/bin/pyenv-installer | bash
RUN $APT_INSTALL libbz2-dev libreadline-dev libsqlite3-dev
RUN pyenv install 3.7.6
RUN pyenv global 3.7.6
RUN python --version
RUN pip install --upgrade pip
RUN pip --version
RUN pip install $PIP_PKGS

ENV PATH "$PATH:/root/.rbenv/bin:/root/.rbenv/shims"
RUN curl -fsSL https://github.com/rbenv/rbenv-installer/raw/108c12307621a0aa06f19799641848dde1987deb/bin/rbenv-installer | bash
RUN rbenv install 2.7.0
RUN rbenv global 2.7.0
RUN ruby --version
RUN $APT_INSTALL g++
RUN gem --version
RUN gem install --no-document $GEM_PKGS

RUN \
  curl -sL https://deb.nodesource.com/setup_11.x | bash && \
  $APT_INSTALL nodejs

# Install R packages and dependencies used when building.
# R depends on pandoc*, libssl (which are installed above).
RUN \
  $APT_INSTALL r-base r-base-dev && \
  $APT_INSTALL texlive-latex-base texlive texlive-fonts-extra texinfo qpdf && \
  Rscript -e "install.packages(c('curl', 'xml2', 'httr', 'devtools', 'testthat', 'knitr', 'rmarkdown', 'roxygen2', 'e1071', 'survival'), repos='https://cloud.r-project.org/')" && \
  Rscript -e "devtools::install_github('jimhester/lintr')"

WORKDIR /opt/spark-rm/output

ARG UID
RUN useradd -m -s /bin/bash -p spark-rm -u $UID spark-rm
USER spark-rm:spark-rm

ENTRYPOINT [ "/opt/spark-rm/do-release.sh" ]
