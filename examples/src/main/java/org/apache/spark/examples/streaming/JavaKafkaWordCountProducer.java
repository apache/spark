/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * words producer to java kafka word count
 * Produces some random words between 1 and 10
 *
 * Usage: JavaKafkaWordCountProducer <metadataBrokerList> <topic> <messagesPerSec> <wordsPerMessage>
 *   <metadataBrokerList> is a list of one or more kafka broker servers
 *   <topic> is a kafka topic to consume from
 *   <messagesPerSec> is the number of messages generated per second
 *   <wordsPerMessage> is the number of words per message
 */
public class JavaKafkaWordCountProducer {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: JavaKafkaWordCountProducer <metadataBrokerList> <topic> " +
                    "<messagesPerSec> <wordsPerMessage>");
            System.exit(1);
        }

        String topic = args[1];
        String messagesPerSec = args[2];
        String wordsPerMessage = args[3];

        // Zookeeper connection properties
        Map props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        // Send some messages
        while (true) {
            for (int i = 1; i <= Integer.valueOf(messagesPerSec); i++) {
                String str = "";
                for (int j = 1; j <= Integer.valueOf(wordsPerMessage); j++) {
                    str = str + String.valueOf(new Random().nextInt(10)) + " ";
                }
                ProducerRecord message = new ProducerRecord<String, String>(topic, null, str);
                producer.send(message);
            }
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
