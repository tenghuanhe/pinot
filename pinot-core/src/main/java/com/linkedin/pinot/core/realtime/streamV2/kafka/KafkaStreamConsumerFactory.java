package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.linkedin.pinot.core.realtime.streamV2.HighLevelStreamConsumer;
import com.linkedin.pinot.core.realtime.streamV2.SimpleStreamConsumer;
import com.linkedin.pinot.core.realtime.streamV2.StreamConfig;
import com.linkedin.pinot.core.realtime.streamV2.StreamConsumerFactory;
import com.linkedin.pinot.core.realtime.streamV2.StreamMetadataProvider;


public class KafkaStreamConsumerFactory extends StreamConsumerFactory {

  public KafkaStreamConsumerFactory(StreamConfig streamConfig) {
    super(streamConfig);
  }

  @Override
  public SimpleStreamConsumer createSimpleConsumer(String clientId, int partition) {
    return new KafkaSimpleStreamConsumer(_streamConfig, clientId, partition);
  }

  @Override
  public HighLevelStreamConsumer createHighLevelConsumer() {
    return new KafkaHighLevelStreamConsumer(_streamConfig);
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new KafkaSimpleStreamMetadataProvider(_streamConfig, clientId);
  }
}
