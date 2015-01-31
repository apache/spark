# Creation of a CentOS with Data Controller

FROM centos-infra-dev3.ord.labs.aspect.com:5000/centos7-jdk7u67x64
MAINTAINER OMNI-CHANNEL-TEAM
RUN echo "Creating the 'Spark Master and Worker' Docker container image file..."

ADD ./* /root/spark1.3

WORKDIR /root/spark1.3

ENTRYPOINT sh startScript.sh
