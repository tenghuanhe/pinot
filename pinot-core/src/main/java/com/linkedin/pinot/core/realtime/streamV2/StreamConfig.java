package com.linkedin.pinot.core.realtime.streamV2;

import com.google.common.base.Joiner;
import com.linkedin.pinot.common.utils.time.TimeUtils;
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

  private static final long DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS = 30_000;
  private static final int DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS = 5_000;

  private String _type;
  private String _name;
  private List<StreamConsumerType> _consumerTypes;
  private long _connectionTimeoutMillis = DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS;
  private long _fetchTimeoutMillis = DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS;
  private int _flushThresholdRows = DEFAULT_FLUSH_THRESHOLD_ROWS;
  private long _flushThresholdTimeMillis = DEFAULT_FLUSH_THRESHOLD_TIME;

  private Map<String, String> _streamConfigMap;

  public StreamConfig(@Nonnull Map<String, String> streamConfigMap) {

    _streamConfigMap = streamConfigMap;
    _type = streamConfigMap.get(StreamConfigProperties.STREAM_TYPE);

    String streamNameProperty = StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.NAME);
    _name = streamConfigMap.get(streamNameProperty);

    String consumerTypes = StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.CONSUMER_TYPES);
    for (String consumerType : consumerTypes.split(",")) {
      _consumerTypes.add(StreamConsumerType.valueOf(consumerType));
    }

    String connectionTimeout = getStreamSpecificValue(StreamConfigProperties.CONNECTION_TIMEOUT_MILLIS);
    if (connectionTimeout != null) {
      _connectionTimeoutMillis = Long.parseLong(connectionTimeout);
    }

    String fetchTimeout = getStreamSpecificValue(StreamConfigProperties.FETCH_TIMEOUT_MILLIS);
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
    // TODO
    // Add desired segment size
    // Look for stream.<name>.decoder.class and set it as a generic property
    // Look for stream.<name>.consumer.factory.class and set it as a generic property
  }

  public String getType() {
    return _type;
  }

  // Rename to getTopicName?
  public String getName() {
    return _name;
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

  public String getValue(String key) {
    return _streamConfigMap.get(key);
  }

  // TODO Remove this method
  public String getStreamSpecificValue(String key) {
    String streamProperty = StreamConfigProperties.constructStreamProperty(_type, key);
    return _streamConfigMap.get(streamProperty);
  }

  public Map<String, String> getStreamSpecificProperties() {
    Map<String, String> streamSpecificProperties = new HashMap<>();
    String streamSpecificPrefix =
        Joiner.on(StreamConfigProperties.DOT_SEPARATOR).join(StreamConfigProperties.STREAM_PREFIX, _type);
    for (String key : _streamConfigMap.keySet()) {
      if (key.startsWith(streamSpecificPrefix)) {
        streamSpecificProperties.put(key, _streamConfigMap.get(key));
      }
    }
    return streamSpecificProperties;
  }

  public Map<String, String> getDecoderProperties() {
    String decoderPropsPrefix =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.DECODER_PROPS_PREFIX);
    Map<String, String> decoderProps = new HashMap<>();
    for (String key : _streamConfigMap.keySet()) {
      if (key.startsWith(decoderPropsPrefix)) {
        decoderProps.put(getDecoderPropertyKey(key), _streamConfigMap.get(key));
      }
    }
    return decoderProps;
  }

  private String getDecoderPropertyKey(String key) {
    return key.split(StreamConfigProperties.DECODER_PROPS_PREFIX + StreamConfigProperties.DOT_SEPARATOR)[1];
  }
}
