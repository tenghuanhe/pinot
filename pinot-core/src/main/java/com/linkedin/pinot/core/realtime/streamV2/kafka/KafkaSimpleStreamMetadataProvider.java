package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.core.realtime.streamV2.StreamConfig;
import com.linkedin.pinot.core.realtime.streamV2.StreamMetadataProvider;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;


public class KafkaSimpleStreamMetadataProvider extends KafkaSimpleStream implements StreamMetadataProvider {

  public KafkaSimpleStreamMetadataProvider(StreamConfig streamConfig, String clientId) {
    super(streamConfig, clientId);
  }

  @Override
  public synchronized int getPartitionCount(int timeoutMillis) {
    int unknownTopicReplyCount = 0;
    int kafkaErrorCount = 0;

    final long endTime = System.currentTimeMillis() + timeoutMillis;

    while (System.currentTimeMillis() < endTime) {
      while (!_currentState.isConnectedToKafkaBroker() && System.currentTimeMillis() < endTime) {
        _currentState.process();
      }

      if (endTime <= System.currentTimeMillis() && !_currentState.isConnectedToKafkaBroker()) {
        throw new TimeoutException(
            "Failed to get the partition count for topic " + _topic + " within " + timeoutMillis + " ms");
      }

      TopicMetadataResponse topicMetadataResponse;
      try {
        topicMetadataResponse = _simpleConsumer.send(new TopicMetadataRequest(Collections.singletonList(_topic)));
      } catch (Exception e) {
        _currentState.handleConsumerException(e);
        continue;
      }

      final TopicMetadata topicMetadata = topicMetadataResponse.topicsMetadata().get(0);
      final short errorCode = topicMetadata.errorCode();

      if (errorCode == Errors.NONE.code()) {
        return topicMetadata.partitionsMetadata().size();
      } else if (errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
        // If there is no leader, it'll take some time for a new leader to be elected, wait 100 ms before retrying
        Uninterruptibles.sleepUninterruptibly(SLEEP_FOR_MILLIS_ON_ERROR, TimeUnit.MILLISECONDS);
      } else if (errorCode == Errors.INVALID_TOPIC_EXCEPTION.code()) {
        throw new RuntimeException("Invalid topic name " + _topic);
      } else if (errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
        unknownTopicReplyCount++;
        if (MAX_UNKNOWN_TOPIC_REPLY_COUNT < unknownTopicReplyCount) {
          throw new RuntimeException("Topic " + _topic + " does not exist");
        }
        // Kafka topic creation can sometimes take some time, so we'll retry after a little bit
        Uninterruptibles.sleepUninterruptibly(SLEEP_FOR_MILLIS_ON_ERROR, TimeUnit.MILLISECONDS);
      } else {
        kafkaErrorCount++;
        if (MAX_KAFKA_ERROR_COUNT < kafkaErrorCount) {
          throw exceptionForKafkaErrorCode(errorCode);
        }
        // Retry after a short delay
        Uninterruptibles.sleepUninterruptibly(SLEEP_FOR_MILLIS_ON_ERROR, TimeUnit.MILLISECONDS);
      }
    }
    throw new TimeoutException();
  }

  @Override
  public void close() throws IOException {
    close();
  }
}
