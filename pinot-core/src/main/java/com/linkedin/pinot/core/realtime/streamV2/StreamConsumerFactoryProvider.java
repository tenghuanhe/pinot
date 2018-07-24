package com.linkedin.pinot.core.realtime.streamV2;

import com.linkedin.pinot.common.Utils;
import java.lang.reflect.Constructor;


/**
 * Provider class which creates the consumer factory from the properties in stream configs
 */
public class StreamConsumerFactoryProvider {
  public static StreamConsumerFactory create(StreamConfig streamConfig) {
    String streamType = streamConfig.getType();

    String streamConsumerFactoryProperty =
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.CONSUMER_FACTORY_CLASS);

    String factoryClassString = streamConfig.getStreamSpecificValue(StreamConfigProperties.CONSUMER_FACTORY_CLASS);
    StreamConsumerFactory factory = null;
    try {
      Class<?> streamConsumerFactoryClass = Class.forName(factoryClassString);
      Constructor<?> constructor = streamConsumerFactoryClass.getConstructor(StreamConfig.class);
      factory = (StreamConsumerFactory) constructor.newInstance(streamConfig);
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
    return factory;
  }
}
