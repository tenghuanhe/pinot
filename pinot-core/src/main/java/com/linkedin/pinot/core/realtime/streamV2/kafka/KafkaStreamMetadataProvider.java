package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.linkedin.pinot.core.realtime.streamV2.StreamConfig;
import com.linkedin.pinot.core.realtime.streamV2.StreamMetadataProvider;
import java.io.IOException;


public class KafkaStreamMetadataProvider implements StreamMetadataProvider {
  @Override
  public int getPartitionCount(StreamConfig streamConfig) {
    return 0;
  }

  @Override
  public long getPartitionOffset(StreamConfig streamConfig, int partitionId, String offsetCriteria) {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
