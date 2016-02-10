
package org.apache.spark.streaming.kinesis


import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}

private[kinesis]
class CredentialPool(
     kinesisCredentials: SerializableAWSCredentials,
     dynamoDBCredentials: SerializableAWSCredentials) {
     
   def getKineisCredentials() = kinesisCredentials
	 def getDynamoDbCredentials() = dynamoDBCredentials
}