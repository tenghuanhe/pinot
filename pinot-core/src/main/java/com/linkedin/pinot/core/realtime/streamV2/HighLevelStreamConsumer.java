package com.linkedin.pinot.core.realtime.streamV2;

import com.linkedin.pinot.common.metrics.ServerMetrics;


public interface HighLevelStreamConsumer extends StreamConsumer {
  void init(ServerMetrics serverMetrics) throws Exception;

  byte[] fetchNextMessage();

  void commit();
}
