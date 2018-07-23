package com.linkedin.pinot.core.realtime.streamV2;

public abstract class StreamConsumerFactory {

  private StreamConfig _streamConfig;

  public StreamConsumerFactory(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
  }

  abstract SimpleStreamConsumer createSimpleConsumer();

  abstract HighLevelStreamConsumer createHighLevelConsumer();

  abstract StreamMetadataProvider createStreamMetadataProvider();
}
