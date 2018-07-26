package com.linkedin.pinot.core.realtime.streamV2;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.DataSize;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;


/**
 * Wrapper around stream configs map from table config.
 */
public class StreamConfig {

  public enum StreamConsumerType {
    HIGH_LEVEL, SIMPLE
  }

  private int DEFAULT_FLUSH_THRESHOLD_ROWS = 5_000_000;
  private long DEFAULT_FLUSH_THRESHOLD_TIME = TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS);
  private static final long DEFAULT_DESIRED_SEGMENT_SIZE_BYTES = 200 * 1024 * 1024;

  private static final long DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS = 30_000;
  private static final int DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS = 5_000;

  private String _type;
  private String _topicName;
  private List<StreamConsumerType> _consumerTypes = new ArrayList<>();
  private long _connectionTimeoutMillis = DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS;
  private long _fetchTimeoutMillis = DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS;
  private int _flushThresholdRows = DEFAULT_FLUSH_THRESHOLD_ROWS;
  private long _flushThresholdTimeMillis = DEFAULT_FLUSH_THRESHOLD_TIME;
  private long _flushSegmentDesiredSizeBytes = DEFAULT_DESIRED_SEGMENT_SIZE_BYTES;

  private String _decoderClass;
  private String _consumerFactoryClass;
  private Map<String, String> _decoderProperties = new HashMap<>();

  Map<String, String> _streamSpecificProperties = new HashMap<>();

  public StreamConfig(@Nonnull Map<String, String> streamConfigMap) {

    _type = streamConfigMap.get(StreamConfigProperties.STREAM_TYPE);
    _topicName = streamConfigMap.get(StreamConfigProperties.STREAM_TOPIC_NAME);

    Preconditions.checkNotNull(_type, "Stream type cannot be null");
    Preconditions.checkNotNull(_topicName, "Stream topic name cannot be null");

    String consumerTypes = streamConfigMap.get(StreamConfigProperties.STREAM_CONSUMER_TYPES);
    Preconditions.checkNotNull(consumerTypes, "Must specify at least one consumer type");
    for (String consumerType : consumerTypes.split(",")) {
      _consumerTypes.add(StreamConsumerType.valueOf(consumerType));
    }

    String connectionTimeout = streamConfigMap.get(StreamConfigProperties.STREAM_CONNECTION_TIMEOUT_MILLIS);
    if (connectionTimeout != null) {
      _connectionTimeoutMillis = Long.parseLong(connectionTimeout);
    }

    String fetchTimeout = streamConfigMap.get(StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS);
    if (fetchTimeout != null) {
      _fetchTimeoutMillis = Long.parseLong(fetchTimeout);
    }

    String flushThresholdRows = streamConfigMap.get(StreamConfigProperties.STREAM_FLUSH_THRESHOLD_ROWS);
    if (flushThresholdRows != null) {
      _flushThresholdRows = Integer.parseInt(flushThresholdRows);
    }

    String flushThresholdTime = streamConfigMap.get(StreamConfigProperties.STREAM_FLUSH_THRESHOLD_TIME);
    if (flushThresholdTime != null) {
      _flushThresholdTimeMillis = TimeUtils.convertPeriodToMillis(flushThresholdTime);
    }
    String flushSegmentDesiredSize = streamConfigMap.get(StreamConfigProperties.STREAM_FLUSH_SEGMENT_DESIRED_SIZE);
    if (flushSegmentDesiredSize != null) {
      _flushSegmentDesiredSizeBytes = DataSize.toBytes(flushSegmentDesiredSize);
    }

    _decoderClass = streamConfigMap.get(StreamConfigProperties.STREAM_DECODER_CLASS);
    Preconditions.checkNotNull(_decoderClass, "Must specify decoder class");

    for (String key : streamConfigMap.keySet()) {
      if (key.startsWith(StreamConfigProperties.STREAM_DECODER_PROPERTIES_PREFIX)) {
        _decoderProperties.put(key, streamConfigMap.get(key));
      }
    }

    _consumerFactoryClass = streamConfigMap.get(StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS);
    Preconditions.checkNotNull(_consumerFactoryClass, "Must specify consumer factory class name");

    String streamSpecificPrefix =
        Joiner.on(StreamConfigProperties.DOT_SEPARATOR).join(StreamConfigProperties.STREAM_PREFIX, _type);
    for (String key : streamConfigMap.keySet()) {
      if (key.startsWith(streamSpecificPrefix)) {
        _streamSpecificProperties.put(key, streamConfigMap.get(key));
      }
    }
  }

  public String getType() {
    return _type;
  }

  public String getTopicName() {
    return _topicName;
  }

  public boolean hasHighLevelConsumer() {
    return _consumerTypes.contains(StreamConsumerType.HIGH_LEVEL);
  }

  public boolean hasSimpleConsumer() {
    return _consumerTypes.contains(StreamConsumerType.SIMPLE);
  }

  public long getConnectionTimeoutMillis() {
    return _connectionTimeoutMillis;
  }

  public long getFetchTimeoutMillis() {
    return _fetchTimeoutMillis;
  }

  public int getFlushThresholdRows() {
    return _flushThresholdRows;
  }

  public long getFlushThresholdTimeMillis() {
    return _flushThresholdTimeMillis;
  }

  public long getFlushSegmentDesiredSizeBytes() {
    return _flushSegmentDesiredSizeBytes;
  }

  public String getDecoderClass() {
    return _decoderClass;
  }

  public Map<String, String> getDecoderProperties() {
    return _decoderProperties;
  }

  public String getConsumerFactoryClass() {
    return _consumerFactoryClass;
  }

  public Map<String, String> getStreamSpecificProperties() {
    return _streamSpecificProperties;
  }
}
