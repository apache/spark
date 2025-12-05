FROM eclipse-temurin:17-jdk

RUN apt-get update && apt-get install -y \
    curl python3 python3-pip git \
    && rm -rf /var/lib/apt/lists/*

ENV MAVEN_OPTS="-Xss64m -Xmx2g -XX:ReservedCodeCacheSize=1g"

WORKDIR /workspace

CMD ["/bin/bash"]