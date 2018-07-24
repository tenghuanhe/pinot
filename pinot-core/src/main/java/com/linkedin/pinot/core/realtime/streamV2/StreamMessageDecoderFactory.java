package com.linkedin.pinot.core.realtime.streamV2;

import com.linkedin.pinot.common.data.Schema;
import java.util.HashMap;
import java.util.Map;


/**
 * Factory class to create a decoder from the stream configs
 */
public class StreamMessageDecoderFactory {

  public StreamMessageDecoder createStreamMessageDecoder(StreamConfig streamConfig, Schema indexingSchema) {
    String streamType = streamConfig.getType();

    String decoderPropsPrefix =
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.DECODER_PROPS_PREFIX);
    Map<String, String> decoderProps = new HashMap<>();
    for (String key : streamConfig.getAllProperties().keySet()) {
      if (key.startsWith(decoderPropsPrefix)) {
        decoderProps.put(getDecoderPropertyKey(key), streamConfig.getValue(key));
      }
    }

    String decoderClassString = streamConfig.getStreamSpecificValue(StreamConfigProperties.DECODER_CLASS);
    StreamMessageDecoder decoder;
    try {
      decoder = (StreamMessageDecoder) Class.forName(decoderClassString).newInstance();
      decoder.init(decoderProps, indexingSchema, streamConfig.getName());
    } catch (Exception e) {
      throw new RuntimeException(
          "Could not instantiate decoder " + decoderClassString + " for stream type " + streamType);
    }
    return decoder;
  }

  private String getDecoderPropertyKey(String key) {
    return key.split(StreamConfigProperties.DECODER_PROPS_PREFIX + StreamConfigProperties.DOT_SEPARATOR)[1];
  }
}
