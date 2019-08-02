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

package org.apache.spark.sql.kafka010;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.requests.FindCoordinatorRequest;

import org.apache.spark.annotation.Evolving;

/**
 * Wrapper around KafkaProducer that allows to resume transactions when restart
 *  in case of application fail between {@link #flush()} and {@link #commitTransaction()}
 */
@Evolving
public class InternalKafkaProducer<K, V> implements Producer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(InternalKafkaProducer.class);

  protected final KafkaProducer<K, V> kafkaProducer;

  @Nullable
  protected final String transactionalId;

  public InternalKafkaProducer(Map<String, Object> configs) {
    if (configs.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) {
      transactionalId = configs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG).toString();
    } else {
      transactionalId = null;
    }
    kafkaProducer = new KafkaProducer<>(configs);
  }

  @Override
  public void initTransactions() {
    kafkaProducer.initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    kafkaProducer.beginTransaction();
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    kafkaProducer.commitTransaction();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    kafkaProducer.abortTransaction();
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                       String consumerGroupId) throws ProducerFencedException {
    kafkaProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return kafkaProducer.send(record);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    return kafkaProducer.send(record, callback);
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return kafkaProducer.partitionsFor(topic);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return kafkaProducer.metrics();
  }

  @Override
  public void close() {
    kafkaProducer.close();
  }

  @Override
  public void close(long timeout, TimeUnit unit) {
    kafkaProducer.close(timeout, unit);
  }

  @Override
  public void close(Duration duration) {
    kafkaProducer.close(duration);
  }

  @Override
  public void flush() {
    kafkaProducer.flush();
  }

  /**
   * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously
   * obtained ones, so that we can resume transaction after a restart. Implementation of
   * this method is based on {@link KafkaProducer#initTransactions}.
   * https://github.com/apache/kafka/commit/5d2422258cb975a137a42a4e08f03573c49a387e#diff-f4ef1afd8792cd2a2e9069cd7ddea630
   */
  public void resumeTransaction(long producerId, short epoch) {
    Preconditions.checkState(producerId >= 0 && epoch >= 0,
        "Incorrect values for producerId %s and epoch %s", producerId, epoch);
    LOG.info("Attempting to resume transaction {} with producerId {} and epoch {}",
        transactionalId, producerId, epoch);

    Object transactionManager = getValue(kafkaProducer, "transactionManager");
    synchronized (transactionManager) {
      invoke(transactionManager, "transitionTo", getEnum(
          "org.apache.kafka.clients.producer.internals.TransactionManager$State.INITIALIZING"));

      Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
      setValue(producerIdAndEpoch, "producerId", producerId);
      setValue(producerIdAndEpoch, "epoch", epoch);

      invoke(transactionManager, "transitionTo", getEnum(
          "org.apache.kafka.clients.producer.internals.TransactionManager$State.READY"));

      invoke(transactionManager, "transitionTo", getEnum(
          "org.apache.kafka.clients.producer.internals.TransactionManager$State.IN_TRANSACTION"));
      setValue(transactionManager, "transactionStarted", true);
    }
  }

  public String getTransactionalId() {
    return transactionalId;
  }

  public long getProducerId() {
    Object transactionManager = getValue(kafkaProducer, "transactionManager");
    Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
    return (long) getValue(producerIdAndEpoch, "producerId");
  }

  public short getEpoch() {
    Object transactionManager = getValue(kafkaProducer, "transactionManager");
    Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
    return (short) getValue(producerIdAndEpoch, "epoch");
  }

  @VisibleForTesting
  public int getTransactionCoordinatorId() {
    Object transactionManager = getValue(kafkaProducer, "transactionManager");
    Node node = (Node) invoke(transactionManager, "coordinator",
        FindCoordinatorRequest.CoordinatorType.TRANSACTION);
    return node.id();
  }

  protected static Enum<?> getEnum(String enumFullName) {
    String[] x = enumFullName.split("\\.(?=[^\\.]+$)");
    if (x.length == 2) {
      String enumClassName = x[0];
      String enumName = x[1];
      try {
        Class<Enum> cl = (Class<Enum>) Class.forName(enumClassName);
        return Enum.valueOf(cl, enumName);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Incompatible KafkaProducer version", e);
      }
    }
    return null;
  }

  protected static Object invoke(Object object, String methodName, Object... args) {
    Class<?>[] argTypes = new Class[args.length];
    for (int i = 0; i < args.length; i++) {
      argTypes[i] = args[i].getClass();
    }
    return invoke(object, methodName, argTypes, args);
  }

  private static Object invoke(Object object, String methodName, Class<?>[] argTypes, Object[] args) {
    try {
      Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
      method.setAccessible(true);
      return method.invoke(object, args);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException("Incompatible KafkaProducer version", e);
    }
  }

  protected static Object getValue(Object object, String fieldName) {
    return getValue(object, object.getClass(), fieldName);
  }

  private static Object getValue(Object object, Class<?> clazz, String fieldName) {
    try {
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(object);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Incompatible KafkaProducer version", e);
    }
  }

  protected static void setValue(Object object, String fieldName, Object value) {
    try {
      Field field = object.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(object, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Incompatible KafkaProducer version", e);
    }
  }
}
