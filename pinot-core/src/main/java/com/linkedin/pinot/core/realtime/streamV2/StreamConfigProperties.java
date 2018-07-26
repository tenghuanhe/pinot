package com.linkedin.pinot.core.realtime.streamV2;

import com.google.common.base.Joiner;


public class StreamConfigProperties {
  public static final String DOT_SEPARATOR = ".";
  public static final String STREAM_PREFIX = "stream";

  /**
   * Stream specific properties
   * These will be used as stream.streamType.property
   * eg. stream.kafka.name, stream.kafka.decoder.class
   */
  public static final String NAME = "name";
  public static final String CONSUMER_TYPES = "consumer.types";
  public static final String DECODER_CLASS = "decoder.class";
  public static final String DECODER_PROPS_PREFIX = "decoder";
  public static final String CONSUMER_FACTORY_CLASS = "consumer.factory.class";
  public static final String CONNECTION_TIMEOUT_MILLIS = "connection.timeout.millis";
  public static final String FETCH_TIMEOUT_MILLIS = "fetch.timeout.millis";

  /**
   * Generic properties
   */
  public static final String STREAM_TYPE = "stream.type";
  public static final String STREAM_FLUSH_THRESHOLD_TIME = "stream.flush.threshold.time";
  public static final String STREAM_FLUSH_THRESHOLD_ROWS = "stream.flush.threshold.rows";
  public static final String STREAM_FLUSH_SEGMENT_SIZE = "segment.flush.desired.size";

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
