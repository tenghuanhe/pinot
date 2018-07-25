package com.linkedin.pinot.core.realtime.streamV2;

/**
 * Factory class to create a decoder from the stream configs
 */
public class StreamMessageDecoderFactory {

  public static StreamMessageDecoder createStreamMessageDecoder(StreamConfig streamConfig) {

    String decoderClassString = streamConfig.getStreamSpecificValue(StreamConfigProperties.DECODER_CLASS);
    StreamMessageDecoder decoder;
    try {
      decoder = (StreamMessageDecoder) Class.forName(decoderClassString).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate decoder " + decoderClassString);
    } return decoder;
  }
}
