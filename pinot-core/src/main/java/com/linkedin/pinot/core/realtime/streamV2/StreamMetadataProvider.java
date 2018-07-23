package com.linkedin.pinot.core.realtime.streamV2;

import java.io.Closeable;


public interface StreamMetadataProvider extends Closeable {

  int getPartitionCount(StreamConfig streamConfig);

  long getPartitionOffset(StreamConfig streamConfig, int partitionId, String offsetCriteria);

}
