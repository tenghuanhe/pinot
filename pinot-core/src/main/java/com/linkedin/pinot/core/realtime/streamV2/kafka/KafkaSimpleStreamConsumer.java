package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerMessageBatch;
import com.linkedin.pinot.core.realtime.stream.MessageBatch;
import com.linkedin.pinot.core.realtime.streamV2.SimpleStreamConsumer;
import com.linkedin.pinot.core.realtime.streamV2.StreamConfig;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaSimpleStreamConsumer extends KafkaConnectionProvider implements SimpleStreamConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSimpleStreamConsumer.class);

  private final int MAX_KAFKA_ERROR_COUNT = 10;

  public KafkaSimpleStreamConsumer(StreamConfig streamConfig, String clientId, int partition, String tableName) {
    super(streamConfig, clientId, partition, tableName);
  }

  /**
   * Fetch messages and the per-partition high watermark from Kafka between the specified offsets.
   *
   * @param startOffset The offset of the first message desired, inclusive
   * @param endOffset The offset of the last message desired, exclusive, or {@link Long#MAX_VALUE} for no end offset.
   * @param timeoutMillis Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An iterable containing messages fetched from Kafka and their offsets, as well as the high watermark for
   * this partition.
   */
  @Override
  public synchronized MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException {

    final long connectEndTime = System.currentTimeMillis() + _connectionTimeoutMillis;
    while (_currentState.getStateValue() != ConsumerState.CONNECTED_TO_PARTITION_LEADER
        && System.currentTimeMillis() < connectEndTime) {
      _currentState.process();
    }
    if (_currentState.getStateValue() != ConsumerState.CONNECTED_TO_PARTITION_LEADER
        && connectEndTime <= System.currentTimeMillis()) {
      throw new java.util.concurrent.TimeoutException();
    }

    FetchResponse fetchResponse = _simpleConsumer.fetch(new FetchRequestBuilder().minBytes(100000)
        .maxWait(timeoutMillis)
        .addFetch(_topic, _partition, startOffset, 500000)
        .build());

    if (!fetchResponse.hasError()) {
      final Iterable<MessageAndOffset> messageAndOffsetIterable =
          buildOffsetFilteringIterable(fetchResponse.messageSet(_topic, _partition), startOffset, endOffset);

      return new SimpleConsumerMessageBatch(messageAndOffsetIterable);
    } else {
      throw exceptionForKafkaErrorCode(fetchResponse.errorCode(_topic, _partition));
    }
  }

  private Iterable<MessageAndOffset> buildOffsetFilteringIterable(final ByteBufferMessageSet messageAndOffsets,
      final long startOffset, final long endOffset) {
    return Iterables.filter(messageAndOffsets, input -> {
      // Filter messages that are either null or have an offset ∉ [startOffset, endOffset]
      if (input == null || input.offset() < startOffset || (endOffset <= input.offset() && endOffset != -1)) {
        return false;
      }

      // Check the message's checksum
      // TODO We might want to have better handling of this situation, maybe try to fetch the message again?
      if (!input.message().isValid()) {
        LOGGER.warn("Discarded message with invalid checksum in partition {} of topic {}", _partition, _topic);
        return false;
      }

      return true;
    });
  }

  /**
   * Fetches the numeric Kafka offset for this partition for a symbolic name ("largest" or "smallest").
   *
   * @param requestedOffset Either "largest" or "smallest"
   * @param timeoutMillis Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An offset
   */
  @Override
  public synchronized long fetchPartitionOffset(String requestedOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException {
    Preconditions.checkNotNull(requestedOffset);

    final long offsetRequestTime;
    if (requestedOffset.equalsIgnoreCase(kafka.api.OffsetRequest.LargestTimeString())) {
      offsetRequestTime = kafka.api.OffsetRequest.LatestTime();
    } else if (requestedOffset.equalsIgnoreCase(kafka.api.OffsetRequest.SmallestTimeString())) {
      offsetRequestTime = kafka.api.OffsetRequest.EarliestTime();
    } else if (requestedOffset.equalsIgnoreCase("testDummy")) {
      return -1L;
    } else {
      throw new IllegalArgumentException("Unknown initial offset value " + requestedOffset);
    }

    int kafkaErrorCount = 0;
    final long endTime = System.currentTimeMillis() + timeoutMillis;

    while (System.currentTimeMillis() < endTime) {
      // Try to get into a state where we're connected to Kafka
      while (_currentState.getStateValue() != ConsumerState.CONNECTED_TO_PARTITION_LEADER
          && System.currentTimeMillis() < endTime) {
        _currentState.process();
      }

      if (_currentState.getStateValue() != ConsumerState.CONNECTED_TO_PARTITION_LEADER
          && endTime <= System.currentTimeMillis()) {
        throw new TimeoutException();
      }

      // Send the offset request to Kafka
      OffsetRequest request = new OffsetRequest(Collections.singletonMap(new TopicAndPartition(_topic, _partition),
          new PartitionOffsetRequestInfo(offsetRequestTime, 1)), kafka.api.OffsetRequest.CurrentVersion(), _clientId);
      OffsetResponse offsetResponse;
      try {
        offsetResponse = _simpleConsumer.getOffsetsBefore(request);
      } catch (Exception e) {
        _currentState.handleConsumerException(e);
        continue;
      }

      final short errorCode = offsetResponse.errorCode(_topic, _partition);

      if (errorCode == Errors.NONE.code()) {
        long offset = offsetResponse.offsets(_topic, _partition)[0];
        if (offset == 0L) {
          LOGGER.warn("Fetched offset of 0 for topic {} and partition {}, is this a newly created topic?", _topic,
              _partition);
        }
        return offset;
      } else if (errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
        // If there is no leader, it'll take some time for a new leader to be elected, wait 100 ms before retrying
        Uninterruptibles.sleepUninterruptibly(SLEEP_FOR_MILLIS_ON_ERROR, TimeUnit.MILLISECONDS);
      } else {
        // Retry after a short delay
        kafkaErrorCount++;
        if (MAX_KAFKA_ERROR_COUNT < kafkaErrorCount) {
          throw exceptionForKafkaErrorCode(errorCode);
        }
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
