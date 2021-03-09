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

# Image for building Spark releases. Based on Ubuntu 16.04.
#
# Includes:
# * Java 8
# * Ivy
# * Python/PyPandoc (2.7.12/3.5.2)
# * R-base/R-base-dev (4.0.2)
# * Ruby 2.3 build utilities

FROM ubuntu:16.04

# These arguments are just for reuse and not really meant to be customized.
ARG APT_INSTALL="apt-get install --no-install-recommends -y"

ARG BASE_PIP_PKGS="setuptools wheel virtualenv"
ARG PIP_PKGS="pyopenssl pypandoc numpy pygments sphinx"

# Install extra needed repos and refresh.
# - CRAN repo
# - Ruby repo (for doc generation)
#
# This is all in a single "RUN" command so that if anything changes, "apt update" is run to fetch
# the most current package versions (instead of potentially using old versions cached by docker).
RUN apt-get clean && apt-get update && $APT_INSTALL gnupg ca-certificates apt-transport-https && \
  echo 'deb https://cloud.r-project.org/bin/linux/ubuntu xenial-cran40/' >> /etc/apt/sources.list && \
  gpg --keyserver keyserver.ubuntu.com --recv-key E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
  gpg -a --export E084DAB9 | apt-key add - && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  apt-get clean && \
  apt-get update && \
  $APT_INSTALL software-properties-common && \
  apt-add-repository -y ppa:brightbox/ruby-ng && \
  apt-get update && \
  # Install openjdk 8.
  $APT_INSTALL openjdk-8-jdk && \
  update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java && \
  # Install build / source control tools
  $APT_INSTALL curl wget git maven ivy subversion make gcc lsof libffi-dev \
    pandoc pandoc-citeproc libssl-dev libcurl4-openssl-dev libxml2-dev && \
  ln -s -T /usr/share/java/ivy.jar /usr/share/ant/lib/ivy.jar && \
  curl -sL https://deb.nodesource.com/setup_4.x | bash && \
  $APT_INSTALL nodejs && \
  # Install needed python packages. Use pip for installing packages (for consistency).
  $APT_INSTALL libpython2.7-dev libpython3-dev python-pip python3-pip && \
  # qpdf is required for CRAN checks to pass.
  $APT_INSTALL qpdf jq && \
  pip install 'setuptools<45' && \
  pip install --upgrade 'pip<21' && hash -r pip && \
  pip install $BASE_PIP_PKGS && \
  pip install $PIP_PKGS && \
  cd && \
  virtualenv -p python3 /opt/p35 && \
  . /opt/p35/bin/activate && \
  pip install setuptools && \
  pip install $BASE_PIP_PKGS && \
  pip install $PIP_PKGS && \
  # Install R packages and dependencies used when building.
  # R depends on pandoc*, libssl (which are installed above).
  $APT_INSTALL r-base r-base-dev && \
  $APT_INSTALL texlive-latex-base texlive texlive-fonts-extra texinfo qpdf && \
  Rscript -e "install.packages(c('curl', 'xml2', 'httr', 'devtools', 'testthat', 'knitr', 'rmarkdown', 'roxygen2', 'e1071', 'survival'), repos='https://cloud.r-project.org/')" && \
  Rscript -e "devtools::install_github('jimhester/lintr')" && \
  # Install tools needed to build the documentation.
  $APT_INSTALL ruby2.3 ruby2.3-dev mkdocs && \
  gem install jekyll --no-rdoc --no-ri -v 3.8.6 && \
  gem install jekyll-redirect-from -v 0.15.0 && \
  gem install pygments.rb

WORKDIR /opt/spark-rm/output

ARG UID
RUN useradd -m -s /bin/bash -p spark-rm -u $UID spark-rm
USER spark-rm:spark-rm

ENTRYPOINT [ "/opt/spark-rm/do-release.sh" ]
