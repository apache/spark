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

# Spark master (4.2-SNAPSHOT) release image
# Extends the base image with:
# - Java 17
# - Python 3.10 with required packages

FROM spark-rm-base:latest

# Install Java 17 for Spark 4.x
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Install Python 3.10
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3.10-dev \
    python3-psutil \
    libpython3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install pip for Python 3.10
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

# Basic Python packages for master
ARG BASIC_PIP_PKGS="numpy pyarrow>=18.0.0 six==1.16.0 pandas==2.3.3 scipy plotly<6.0.0 \
    mlflow>=2.8.1 coverage matplotlib openpyxl memory-profiler>=0.61.0 scikit-learn>=1.3.2 twine==3.4.1"

# Python deps for Spark Connect
ARG CONNECT_PIP_PKGS="grpcio==1.76.0 grpcio-status==1.76.0 protobuf==6.33.5 \
    googleapis-common-protos==1.71.0 graphviz==0.20.3"

# Install Python 3.10 packages
RUN python3.10 -m pip install --ignore-installed 'blinker>=1.6.2' && \
    python3.10 -m pip install --ignore-installed 'six==1.16.0' && \
    python3.10 -m pip install $BASIC_PIP_PKGS unittest-xml-reporting $CONNECT_PIP_PKGS && \
    python3.10 -m pip install 'torch<2.6.0' torchvision --index-url https://download.pytorch.org/whl/cpu && \
    python3.10 -m pip install deepspeed torcheval && \
    python3.10 -m pip cache purge

# Sphinx and documentation packages
# Should unpin 'sphinxcontrib-*' after upgrading sphinx>5
# See 'ipython_genutils' in SPARK-38517, 'docutils<0.18.0' in SPARK-39421
RUN python3.10 -m pip install 'sphinx==4.5.0' mkdocs 'pydata_sphinx_theme>=0.13' \
    sphinx-copybutton nbsphinx numpydoc jinja2 markupsafe 'pyzmq<24.0.0' \
    ipython ipython_genutils sphinx_plotly_directive 'numpy>=1.22' pyarrow pandas \
    'plotly>=4.8' 'docutils<0.18.0' 'flake8==3.9.0' 'mypy==1.8.0' 'pytest==7.1.3' \
    'pytest-mypy-plugins==1.9.3' 'black==23.12.1' 'pandas-stubs==1.2.0.53' \
    'grpcio==1.76.0' 'grpc-stubs==1.24.11' 'googleapis-common-protos-stubs==2.2.0' \
    'sphinxcontrib-applehelp==1.0.4' 'sphinxcontrib-devhelp==1.0.2' \
    'sphinxcontrib-htmlhelp==2.0.1' 'sphinxcontrib-qthelp==1.0.3' \
    'sphinxcontrib-serializinghtml==1.1.5'

# Set Python 3.10 as the default
RUN ln -sf "$(which python3.10)" "/usr/local/bin/python" && \
    ln -sf "$(which python3.10)" "/usr/local/bin/python3"

# Create user for release manager
ARG UID
RUN useradd -m -s /bin/bash -p spark-rm -u $UID spark-rm
USER spark-rm:spark-rm

ENTRYPOINT [ "/opt/spark-rm/do-release.sh" ]
