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
from typing import overload, Callable, Optional, TypeVar, Union

from pyspark.serializers import NoOpSerializer
from pyspark.storagelevel import StorageLevel
from pyspark.streaming import DStream
from pyspark.streaming.context import StreamingContext
from pyspark.util import _print_missing_jar


__all__ = ["KinesisUtils", "InitialPositionInStream", "utf8_decoder"]

T = TypeVar("T")


def utf8_decoder(s: Optional[bytes]) -> Optional[str]:
    """Decode the unicode as UTF-8"""
    if s is None:
        return None
    return s.decode("utf-8")


class KinesisUtils:
    @staticmethod
    @overload
    def createStream(
        ssc: StreamingContext,
        kinesisAppName: str,
        streamName: str,
        endpointUrl: str,
        regionName: str,
        initialPositionInStream: str,
        checkpointInterval: int,
        storageLevel: StorageLevel = ...,
        awsAccessKeyId: Optional[str] = ...,
        awsSecretKey: Optional[str] = ...,
        *,
        stsAssumeRoleArn: Optional[str] = ...,
        stsSessionName: Optional[str] = ...,
        stsExternalId: Optional[str] = ...,
    ) -> "DStream[Optional[str]]":
        ...

    @staticmethod
    @overload
    def createStream(
        ssc: StreamingContext,
        kinesisAppName: str,
        streamName: str,
        endpointUrl: str,
        regionName: str,
        initialPositionInStream: str,
        checkpointInterval: int,
        storageLevel: StorageLevel = ...,
        awsAccessKeyId: Optional[str] = ...,
        awsSecretKey: Optional[str] = ...,
        decoder: Callable[[Optional[bytes]], T] = ...,
        stsAssumeRoleArn: Optional[str] = ...,
        stsSessionName: Optional[str] = ...,
        stsExternalId: Optional[str] = ...,
    ) -> "DStream[T]":
        ...

    @staticmethod
    def createStream(
        ssc: StreamingContext,
        kinesisAppName: str,
        streamName: str,
        endpointUrl: str,
        regionName: str,
        initialPositionInStream: str,
        checkpointInterval: int,
        storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2,
        awsAccessKeyId: Optional[str] = None,
        awsSecretKey: Optional[str] = None,
        decoder: Union[
            Callable[[Optional[bytes]], T], Callable[[Optional[bytes]], Optional[str]]
        ] = utf8_decoder,
        stsAssumeRoleArn: Optional[str] = None,
        stsSessionName: Optional[str] = None,
        stsExternalId: Optional[str] = None,
    ) -> Union["DStream[Union[T, Optional[str]]]", "DStream[T]"]:
        """
        Create an input stream that pulls messages from a Kinesis stream. This uses the
        Kinesis Client Library (KCL) to pull messages from Kinesis.

        Parameters
        ----------
        ssc : :class:`StreamingContext`
            StreamingContext object
        kinesisAppName : str
            Kinesis application name used by the Kinesis Client Library (KCL) to
            update DynamoDB
        streamName : str
            Kinesis stream name
        endpointUrl : str
            Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
        regionName : str
            Name of region used by the Kinesis Client Library (KCL) to update
            DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
        initialPositionInStream : int
            In the absence of Kinesis checkpoint info, this is the
            worker's initial starting position in the stream. The
            values are either the beginning of the stream per Kinesis'
            limit of 24 hours (InitialPositionInStream.TRIM_HORIZON) or
            the tip of the stream (InitialPositionInStream.LATEST).
        checkpointInterval : int
            Checkpoint interval(in seconds) for Kinesis checkpointing. See the Kinesis
            Spark Streaming documentation for more details on the different
            types of checkpoints.
        storageLevel : :class:`pyspark.StorageLevel`, optional
            Storage level to use for storing the received objects (default is
            StorageLevel.MEMORY_AND_DISK_2)
        awsAccessKeyId : str, optional
            AWS AccessKeyId (default is None. If None, will use
            DefaultAWSCredentialsProviderChain)
        awsSecretKey : str, optional
            AWS SecretKey (default is None. If None, will use
            DefaultAWSCredentialsProviderChain)
        decoder : function, optional
            A function used to decode value (default is utf8_decoder)
        stsAssumeRoleArn : str, optional
            ARN of IAM role to assume when using STS sessions to read from
            the Kinesis stream (default is None).
        stsSessionName : str, optional
            Name to uniquely identify STS sessions used to read from Kinesis
            stream, if STS is being used (default is None).
        stsExternalId : str, optional
            External ID that can be used to validate against the assumed IAM
            role's trust policy, if STS is being used (default is None).

        Returns
        -------
        A DStream object

        Notes
        -----
        The given AWS credentials will get saved in DStream checkpoints if checkpointing
        is enabled. Make sure that your checkpoint directory is secure.
        """
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
        jduration = ssc._jduration(checkpointInterval)

        jvm = ssc._jvm
        assert jvm is not None

        try:
            helper = jvm.org.apache.spark.streaming.kinesis.KinesisUtilsPythonHelper()
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                _print_missing_jar(
                    "Streaming's Kinesis",
                    "streaming-kinesis-asl",
                    "streaming-kinesis-asl-assembly",
                    ssc.sparkContext.version,
                )
            raise
        jstream = helper.createStream(
            ssc._jssc,
            kinesisAppName,
            streamName,
            endpointUrl,
            regionName,
            initialPositionInStream,
            jduration,
            jlevel,
            awsAccessKeyId,
            awsSecretKey,
            stsAssumeRoleArn,
            stsSessionName,
            stsExternalId,
        )
        stream: DStream = DStream(jstream, ssc, NoOpSerializer())
        return stream.map(lambda v: decoder(v))


class InitialPositionInStream:
    LATEST, TRIM_HORIZON = (0, 1)
