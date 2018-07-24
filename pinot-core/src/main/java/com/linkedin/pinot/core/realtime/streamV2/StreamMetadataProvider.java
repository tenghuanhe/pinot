package com.linkedin.pinot.core.realtime.streamV2;

import java.io.Closeable;


public interface StreamMetadataProvider extends Closeable {

  int getPartitionCount(int timeoutMillis);

}
