package com.linkedin.pinot.core.realtime.streamV2;

import com.google.common.base.Joiner;


public class StreamConfigProperties {
  public static final String DOT_SEPARATOR = ".";
  public static final String STREAM_PREFIX = "stream";
  public static final String STREAM_DECODER_PROPERTIES_PREFIX = "stream.decoder";

  /**
   * Generic properties
   */
  public static final String STREAM_TYPE = "stream.type";
  public static final String STREAM_TOPIC_NAME = "topic.name";
  public static final String STREAM_CONSUMER_TYPES = "stream.consumer.types";
  public static final String STREAM_CONSUMER_FACTORY_CLASS = "stream.consumer.factory.class";
  public static final String STREAM_DECODER_CLASS = "stream.decoder.class";
  public static final String STREAM_FLUSH_THRESHOLD_TIME = "stream.flush.threshold.time";
  public static final String STREAM_FLUSH_THRESHOLD_ROWS = "stream.flush.threshold.rows";
  public static final String STREAM_FLUSH_SEGMENT_DESIRED_SIZE = "stream.flush.segment.desired.size";
  public static final String STREAM_CONNECTION_TIMEOUT_MILLIS = "stream.connection.timeout.millis";
  public static final String STREAM_FETCH_TIMEOUT_MILLIS = "stream.fetch.timeout.millis";

  /**
   * Helper method to create a stream specific property
   * @param streamType
   * @param property
   * @return
   */
  public static String constructStreamProperty(String streamType, String property) {
    return Joiner.on(DOT_SEPARATOR).join(STREAM_PREFIX, streamType, property);
  }

}
