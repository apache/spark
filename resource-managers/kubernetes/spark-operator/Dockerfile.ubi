FROM quay.io/radanalyticsio/ubi-jre-1.8.0-minimal:1.0

ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=2 -XshowSettings:vm"

LABEL BASE_IMAGE_2="quay.io/radanalyticsio/ubi-jre-1.8.0-minimal:1.0"

ADD target/spark-operator-*.jar /spark-operator.jar

CMD ["/usr/bin/java", "-jar", "/spark-operator.jar"]
