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

# Spark 3.5 release image
# Extends the base image with:
# - Java 8
# - Python 3.8 with required packages

FROM spark-rm-base:latest

# Install Java 8 for Spark 3.x
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Install Python 3.8 from deadsnakes PPA
RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && apt-get install -y \
    python3.8 \
    python3.8-dev \
    python3.8-distutils \
    && rm -rf /var/lib/apt/lists/*

# Install pip for Python 3.8 (using version-specific URL)
RUN curl -sS https://bootstrap.pypa.io/pip/3.8/get-pip.py | python3.8

# Python packages for Spark 3.5
# Based on the original branch-3.5 Dockerfile
ARG PIP_PKGS="sphinx==3.0.4 mkdocs==1.1.2 numpy==1.20.3 pydata_sphinx_theme==0.8.0 \
    ipython==7.19.0 nbsphinx==0.8.0 numpydoc==1.1.0 jinja2==2.11.3 twine==3.4.1 \
    sphinx-plotly-directive==0.1.3 sphinx-copybutton==0.5.2 pandas==2.0.3 pyarrow==4.0.0 \
    plotly==5.4.0 markupsafe==2.0.1 docutils==0.16 grpcio==1.56.0 protobuf==4.21.6 \
    grpcio-status==1.56.0 googleapis-common-protos==1.56.4"

# Install Python 3.8 packages
RUN python3.8 -m pip install --ignore-installed $PIP_PKGS

# Set Python 3.8 as the default
RUN ln -sf "$(which python3.8)" "/usr/local/bin/python" && \
    ln -sf "$(which python3.8)" "/usr/local/bin/python3"

# Create user for release manager
ARG UID
RUN useradd -m -s /bin/bash -p spark-rm -u $UID spark-rm
USER spark-rm:spark-rm

ENTRYPOINT [ "/opt/spark-rm/do-release.sh" ]
