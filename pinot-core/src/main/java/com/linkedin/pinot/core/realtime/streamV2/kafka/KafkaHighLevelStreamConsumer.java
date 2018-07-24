package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.streamV2.HighLevelStreamConsumer;
import java.io.IOException;


public class KafkaHighLevelStreamConsumer implements HighLevelStreamConsumer {
  @Override
  public void init(StreamProviderConfig streamProviderConfig, String tableName, ServerMetrics serverMetrics)
      throws Exception {

  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public void setOffset(long offset) {

  }

  @Override
  public GenericRow next(GenericRow destination) {
    return null;
  }

  @Override
  public GenericRow next(long offset) {
    return null;
  }

  @Override
  public long currentOffset() {
    return 0;
  }

  @Override
  public void commit() {

  }

  @Override
  public void commit(long offset) {

  }

  @Override
  public void close() throws IOException {

  }
}
