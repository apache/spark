FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# Install Java 8, Scala, and build tools
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    scala \
    git \
    curl \
    wget \
    python3 \
    python3-pip \
    maven \
    vim \
    less \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Install sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt

# Install Claude CLI
RUN npm install -g @anthropic-ai/claude

# Set JAVA_HOME for Java 8
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set Spark home to the mounted workspace
ENV SPARK_HOME=/workspace/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/dist/bin

WORKDIR /workspace/spark

# Create unrestricted permissions config
RUN mkdir -p /root/.claude && \
    echo '{"permissions": {"allow": ["Bash(*)", "Read(**)", "Write(**)", "Edit(**)", "BashOutput(*)"], "deny": [], "ask": []}}' > /root/.claude/config.json

CMD ["/bin/bash"]