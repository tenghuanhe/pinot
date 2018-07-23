package com.linkedin.pinot.core.realtime.streamV2;

import com.linkedin.pinot.core.realtime.stream.MessageBatch;


public interface SimpleStreamConsumer extends StreamConsumer {

  MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException;

}
