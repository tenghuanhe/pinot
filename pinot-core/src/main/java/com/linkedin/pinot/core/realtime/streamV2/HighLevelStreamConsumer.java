package com.linkedin.pinot.core.realtime.streamV2;

import com.linkedin.pinot.common.metrics.ServerMetrics;


public interface HighLevelStreamConsumer extends StreamConsumer {
  // TODO remove tablename from this
  void init(String tableName, ServerMetrics serverMetrics) throws Exception;

  byte[] fetchNextMessage();

  void commit();
}
