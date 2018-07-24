package com.linkedin.pinot.core.realtime.streamV2;

import java.util.concurrent.TimeoutException;


public interface SimpleStreamConsumer extends StreamConsumer {

  MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException;


  long fetchPartitionOffset(String offsetCriteria,  int timeoutMillis) throws TimeoutException;
}
