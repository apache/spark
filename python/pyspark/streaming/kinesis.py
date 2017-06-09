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

from py4j.protocol import Py4JJavaError

from pyspark.serializers import PairDeserializer, NoOpSerializer
from pyspark.storagelevel import StorageLevel
from pyspark.streaming import DStream

__all__ = ['KinesisUtils', 'InitialPositionInStream', 'utf8_decoder']


def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('utf-8')


class KinesisUtils(object):

    @staticmethod
    def createStream(ssc, kinesisAppName, streamName, endpointUrl, regionName,
                     initialPositionInStream, checkpointInterval,
                     storageLevel=StorageLevel.MEMORY_AND_DISK_2,
                     awsAccessKeyId=None, awsSecretKey=None, decoder=utf8_decoder,
                     stsAssumeRoleArn=None, stsSessionName=None, stsExternalId=None):
        """
        Create an input stream that pulls messages from a Kinesis stream. This uses the
        Kinesis Client Library (KCL) to pull messages from Kinesis.

        .. note:: The given AWS credentials will get saved in DStream checkpoints if checkpointing
            is enabled. Make sure that your checkpoint directory is secure.

        :param ssc:  StreamingContext object
        :param kinesisAppName:  Kinesis application name used by the Kinesis Client Library (KCL) to
                                update DynamoDB
        :param streamName:  Kinesis stream name
        :param endpointUrl:  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
        :param regionName:  Name of region used by the Kinesis Client Library (KCL) to update
                            DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
        :param initialPositionInStream:  In the absence of Kinesis checkpoint info, this is the
                                         worker's initial starting position in the stream. The
                                         values are either the beginning of the stream per Kinesis'
                                         limit of 24 hours (InitialPositionInStream.TRIM_HORIZON) or
                                         the tip of the stream (InitialPositionInStream.LATEST).
        :param checkpointInterval:  Checkpoint interval for Kinesis checkpointing. See the Kinesis
                                    Spark Streaming documentation for more details on the different
                                    types of checkpoints.
        :param storageLevel:  Storage level to use for storing the received objects (default is
                              StorageLevel.MEMORY_AND_DISK_2)
        :param awsAccessKeyId:  AWS AccessKeyId (default is None. If None, will use
                                DefaultAWSCredentialsProviderChain)
        :param awsSecretKey:  AWS SecretKey (default is None. If None, will use
                              DefaultAWSCredentialsProviderChain)
        :param decoder:  A function used to decode value (default is utf8_decoder)
        :param stsAssumeRoleArn: ARN of IAM role to assume when using STS sessions to read from
                                 the Kinesis stream (default is None).
        :param stsSessionName: Name to uniquely identify STS sessions used to read from Kinesis
                               stream, if STS is being used (default is None).
        :param stsExternalId: External ID that can be used to validate against the assumed IAM
                              role's trust policy, if STS is being used (default is None).
        :return: A DStream object
        """
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
        jduration = ssc._jduration(checkpointInterval)

        try:
            # Use KinesisUtilsPythonHelper to access Scala's KinesisUtils
            helper = ssc._jvm.org.apache.spark.streaming.kinesis.KinesisUtilsPythonHelper()
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                KinesisUtils._printErrorMsg(ssc.sparkContext)
            raise
        jstream = helper.createStream(ssc._jssc, kinesisAppName, streamName, endpointUrl,
                                      regionName, initialPositionInStream, jduration, jlevel,
                                      awsAccessKeyId, awsSecretKey, stsAssumeRoleArn,
                                      stsSessionName, stsExternalId)
        stream = DStream(jstream, ssc, NoOpSerializer())
        return stream.map(lambda v: decoder(v))

    @staticmethod
    def _printErrorMsg(sc):
        print("""
________________________________________________________________________________________________

  Spark Streaming's Kinesis libraries not found in class path. Try one of the following.

  1. Include the Kinesis library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-kinesis-asl-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-kinesis-asl-assembly.jar> ...

________________________________________________________________________________________________

""" % (sc.version, sc.version))


class InitialPositionInStream(object):
    LATEST, TRIM_HORIZON = (0, 1)
