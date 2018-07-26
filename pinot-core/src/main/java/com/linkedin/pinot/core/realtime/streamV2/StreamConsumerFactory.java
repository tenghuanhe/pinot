package com.linkedin.pinot.core.realtime.streamV2;

public abstract class StreamConsumerFactory {

  protected StreamConfig _streamConfig;

  public StreamConsumerFactory(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
  }

  public abstract SimpleStreamConsumer createSimpleConsumer(String clientId, int partition, String tableName);

  public abstract HighLevelStreamConsumer createHighLevelConsumer(String tableName);

  public abstract StreamMetadataProvider createStreamMetadataProvider(String clientId, String tableName);
}
