# Creation of a CentOS with Data Controller

FROM centos-infra-dev3.ord2012.labs.aspect.com:5000/centos7base-jdk7u67-python342
MAINTAINER OMNI-CHANNEL-TEAM
RUN echo "Creating the 'Spark Master and Worker' Docker container image file..."

ADD spark /root/spark1.3

WORKDIR /root/spark1.3

ENTRYPOINT sh startScript.sh
