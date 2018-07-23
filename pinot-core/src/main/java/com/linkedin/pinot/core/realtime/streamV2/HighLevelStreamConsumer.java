package com.linkedin.pinot.core.realtime.streamV2;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;


public interface HighLevelStreamConsumer extends StreamConsumer {
  void init(StreamProviderConfig streamProviderConfig, String tableName, ServerMetrics serverMetrics) throws Exception;

  void start() throws Exception;

  void setOffset(long offset);

  GenericRow next(GenericRow destination);

  GenericRow next(long offset);

  long currentOffset();

  void commit();

  void commit(long offset);
}
