package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.realtime.streamV2.StreamConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;


public class KafkaStreamConfig {
  private static final Map<String, String> defaultProps;
  static {
    defaultProps = new HashMap<>();
    defaultProps.put("zookeeper.session.timeout.ms", "30000");
    defaultProps.put("zookeeper.connection.timeout.ms", "10000");
    defaultProps.put("zookeeper.sync.time.ms", "2000");

    // Rebalance retries will take up to 1 mins to fail.
    defaultProps.put("rebalance.max.retries", "30");
    defaultProps.put("rebalance.backoff.ms", "2000");

    defaultProps.put("auto.commit.enable", "false");
    defaultProps.put(CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET, "largest");
  }

  private Map<String, String> _kafkaStreamProperties;
  private StreamConfig _streamConfig;

  private String _kafkaTopicName;
  private String _groupId;
  private String _bootstrapHosts;
  private String _zkBrokerUrl;
  private Map<String, String> _kafkaConsumerProperties;

  public KafkaStreamConfig(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
    _kafkaStreamProperties = streamConfig.getStreamSpecificProperties();

    _kafkaTopicName = streamConfig.getName();
    if (streamConfig.hasHighLevelConsumer()) {
      _zkBrokerUrl = streamConfig.getStreamSpecificValue(KafkaStreamConfigProperties.HLC_ZK_BROKER_URL);
      _groupId = null;
    }
    if (streamConfig.hasSimpleConsumer()) {
      _bootstrapHosts = streamConfig.getStreamSpecificValue(KafkaStreamConfigProperties.LLC_BROKER_LIST);
    }

    _kafkaConsumerProperties = new HashMap<>();
    String kafkaConsumerPropertyPrefix = KafkaStreamConfigProperties.constructConsumerPropertyPrefix();
    for (String key : _kafkaStreamProperties.keySet()) {
      if (key.startsWith(kafkaConsumerPropertyPrefix)) {
        _kafkaConsumerProperties.put(key, _kafkaStreamProperties.get(key));
      }
    }
  }

  public String getKafkaTopicName() {
    return _kafkaTopicName;
  }

  public Map<String, Integer> getTopicMap(int numThreads) {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(_kafkaTopicName, numThreads);
    return topicCountMap;
  }

  public String getGroupId() {
    return null;
  }

  public String getZkBrokerUrl() {
    return _zkBrokerUrl;
  }

  public String getBootstrapHosts() {
    return _bootstrapHosts;
  }

  public Map<String, String> getKafkaConsumerProperties() {
    return _kafkaConsumerProperties;
  }

  public ConsumerConfig getKafkaConsumerConfig() {
    Properties props = new Properties();
    for (String key : defaultProps.keySet()) {
      props.put(key, defaultProps.get(key));
    }
    for (String key : _kafkaConsumerProperties.keySet()) {
      props.put(key, _kafkaConsumerProperties.get(key));
    }
    props.put("group.id", _groupId);
    props.put("zookeeper.connect", _zkBrokerUrl);

    return new ConsumerConfig(props);
  }

}
