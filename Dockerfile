# Stage 1: Build Spark from source
FROM public.ecr.aws/docker/library/maven:3.9.9-amazoncorretto-17 AS builder

WORKDIR /build

# Copy the source code
COPY . .

ENV MAVEN_OPTS="-Xss64m -Xmx2g -XX:ReservedCodeCacheSize=1g -T 1C"

# Build Spark using the provided Maven wrapper and command
# It might take a significant amount of time and resources.
RUN --mount=type=cache,target=/root/.m2 ./dev/make-distribution.sh --name custom-spark -Phadoop-cloud -DskipTests -Dmaven.javadoc.skip=true
# Minimal build for History Server, includes hadoop-cloud for S3/etc log reading
# Uses cache mount for Maven repo and skips tests/docs



# Stage 2: Runtime image
FROM eclipse-temurin:17-jre

ENV SPARK_HOME=/opt/spark
# Keep the history server running in the foreground
ENV SPARK_NO_DAEMONIZE=true

WORKDIR ${SPARK_HOME}

# Create a directory for the extra jars
RUN mkdir -p $SPARK_HOME/jars

# Download the required jars
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $SPARK_HOME/jars/
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P $SPARK_HOME/jars/

# Copy the built Spark distribution from the builder stage
# Assumes the build output directory in the builder stage is /build/dist
# Adjust the source path "/build/dist" if your build process outputs elsewhere
COPY --from=builder /build/dist .

# Default Spark History Server port
EXPOSE 18080

# Command to start the history server
CMD ["/opt/spark/sbin/start-history-server.sh"] 