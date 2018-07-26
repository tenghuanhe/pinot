package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.google.common.base.Joiner;
import com.linkedin.pinot.core.realtime.streamV2.StreamConfigProperties;


public class KafkaStreamConfigProperties {

  public static final String STREAM_TYPE = "kafka";

  public static final String CONSUMER_PROPS_PREFIX = "consumer";

  public static final String HLC_ZK_BROKER_URL = "hlc.zk.broker.url";
  public static final String LLC_BROKER_LIST = "broker.list";

  public static String constructConsumerPropertyPrefix() {
    return Joiner.on(StreamConfigProperties.DOT_SEPARATOR)
        .join(StreamConfigProperties.STREAM_PREFIX, STREAM_TYPE, CONSUMER_PROPS_PREFIX);
  }

  public static String constructStreamProperty(String property) {
    return Joiner.on(StreamConfigProperties.DOT_SEPARATOR)
        .join(StreamConfigProperties.STREAM_PREFIX, STREAM_TYPE, property);
  }
}
