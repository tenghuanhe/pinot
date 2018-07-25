package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.realtime.streamV2.HighLevelStreamConsumer;
import com.linkedin.pinot.core.realtime.streamV2.StreamConfig;
import com.yammer.metrics.core.Meter;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaHighLevelStreamConsumer implements HighLevelStreamConsumer {
  private static final Logger STATIC_LOGGER = LoggerFactory.getLogger(KafkaHighLevelStreamConsumer.class);
  private Logger INSTANCE_LOGGER = STATIC_LOGGER;
  private KafkaStreamConfig _kafkaStreamConfig;

  private ConsumerConnector _consumer;
  private ConsumerIterator<byte[], byte[]> _consumerIterator;
  private KafkaConsumerAndIterator _kafkaConsumerAndIterator;

  private long _lastLogTime = 0;
  private long _lastCount = 0;
  private long _currentCount = 0L;

  private ServerMetrics _serverMetrics;
  private String _tableAndStreamName;
  private Meter _tableAndStreamRowsConsumed = null;
  private Meter _tableRowsConsumed = null;

  @Override
  public void init(StreamConfig streamConfig, String tableName, ServerMetrics serverMetrics) throws Exception {
    _tableAndStreamName = tableName + "-" + streamConfig.getName();
    INSTANCE_LOGGER = LoggerFactory.getLogger(
        KafkaHighLevelStreamConsumer.class.getName() + "_" + tableName + "_" + streamConfig.getName());
    _serverMetrics = serverMetrics;

    // TODO: how do we get group id?
    _kafkaStreamConfig = new KafkaStreamConfig(streamConfig);
    _kafkaConsumerAndIterator = KafkaConsumerManager.acquireConsumerAndIteratorForConfig(_kafkaStreamConfig);
    _consumerIterator = _kafkaConsumerAndIterator.getIterator();
    _consumer = _kafkaConsumerAndIterator.getConsumer();
  }

  @Override
  public byte[] fetchNextMessage() {
    if (_consumerIterator.hasNext()) {
      try {
        byte[] message = _consumerIterator.next().message();
        _tableAndStreamRowsConsumed =
            _serverMetrics.addMeteredTableValue(_tableAndStreamName, ServerMeter.REALTIME_ROWS_CONSUMED, 1L,
                _tableAndStreamRowsConsumed);
        _tableRowsConsumed =
            _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_ROWS_CONSUMED, 1L, _tableRowsConsumed);
        ++_currentCount;

        final long now = System.currentTimeMillis();
        // Log every minute or 100k events
        if (now - _lastLogTime > 60000 || _currentCount - _lastCount >= 100000) {
          if (_lastCount == 0) {
            INSTANCE_LOGGER.info("Consumed {} events from kafka stream {}", _currentCount,
                _kafkaStreamConfig.getKafkaTopicName());
          } else {
            INSTANCE_LOGGER.info("Consumed {} events from kafka stream {} (rate:{}/s)", _currentCount - _lastCount,
                _kafkaStreamConfig.getKafkaTopicName(),
                (float) (_currentCount - _lastCount) * 1000 / (now - _lastLogTime));
          }
          _lastCount = _currentCount;
          _lastLogTime = now;
        }
        return message;
      } catch (Exception e) {
        INSTANCE_LOGGER.warn("Caught exception while consuming events", e);
        _serverMetrics.addMeteredTableValue(_tableAndStreamName, ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
        _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
        throw e;
      }
    }
    return null;
  }

  @Override
  public void commit() {
    _consumer.commitOffsets();
    _serverMetrics.addMeteredTableValue(_tableAndStreamName, ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
    _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
  }

  @Override
  public void close() {
    if (_kafkaConsumerAndIterator != null) {
      _consumerIterator = null;
      _consumer = null;

      KafkaConsumerManager.releaseConsumerAndIterator(_kafkaConsumerAndIterator);
      _kafkaConsumerAndIterator = null;
    }
  }
}
