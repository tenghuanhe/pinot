package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.linkedin.pinot.core.realtime.streamV2.StreamConsumerExceptions;
import org.apache.kafka.common.protocol.Errors;


public class KafkaStreamConsumerExceptions extends StreamConsumerExceptions {

  /**
   * A Kafka protocol error that indicates a situation that is not likely to clear up by retrying the request (for
   * example, no such topic or offset out of range).
   */
  public static class KafkaPermanentConsumerException extends PermanentConsumerException {
    public KafkaPermanentConsumerException(Errors error) {
      super(error.exception());
    }
  }

  /**
   * A Kafka protocol error that indicates a situation that is likely to be transient (for example, network error or
   * broker not available).
   */
  public static class KafkaTransientConsumerException extends TransientConsumerException {
    public KafkaTransientConsumerException(Errors error) {
      super(error.exception());
    }
  }
}
