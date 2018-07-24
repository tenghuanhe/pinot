package com.linkedin.pinot.core.realtime.streamV2.kafka;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaBrokerWrapper;
import com.linkedin.pinot.core.realtime.streamV2.MessageBatch;
import com.linkedin.pinot.core.realtime.streamV2.SimpleStreamConsumer;
import com.linkedin.pinot.core.realtime.streamV2.StreamConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaSimpleStreamConsumer implements SimpleStreamConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSimpleStreamConsumer.class);
  private static final int SOCKET_TIMEOUT_MILLIS = 10000;
  private static final int SOCKET_BUFFER_SIZE = 512000;
  private static final String BROKER_LIST = "broker.list";

  private enum ConsumerState {
    CONNECTING_TO_BOOTSTRAP_NODE,
    CONNECTED_TO_BOOTSTRAP_NODE,
    FETCHING_LEADER_INFORMATION,
    CONNECTING_TO_PARTITION_LEADER,
    CONNECTED_TO_PARTITION_LEADER
  }

  private State _currentState;

  private final String _clientId;
  private final String _topic;
  private final int _partition;
  private final long _connectionTimeoutMillis;
  private String[] _bootstrapHosts;
  private int[] _bootstrapPorts;
  private SimpleConsumer _simpleConsumer;
  private final Random _random = new Random();
  private KafkaBrokerWrapper _leader;
  private String _currentHost;
  private int _currentPort;

  /**
   * A Kafka protocol error that indicates a situation that is not likely to clear up by retrying the request (for
   * example, no such topic or offset out of range).
   */
  public static class PermanentConsumerException extends RuntimeException {
    public PermanentConsumerException(Errors error) {
      super(error.exception());
    }
  }

  /**
   * A Kafka protocol error that indicates a situation that is likely to be transient (for example, network error or
   * broker not available).
   */
  public static class TransientConsumerException extends RuntimeException {
    public TransientConsumerException(Errors error) {
      super(error.exception());
    }
  }

  public KafkaSimpleStreamConsumer(StreamConfig streamConfig, String clientId, int partition) {
    _clientId = clientId;
    _topic = streamConfig.getName();
    _partition = partition;
    _connectionTimeoutMillis = streamConfig.getConnectionTimeoutMillis();
    _simpleConsumer = null;
    String bootstrapNodes = streamConfig.getStreamSpecificValue(BROKER_LIST);
    initializeBootstrapNodeList(bootstrapNodes);
    setCurrentState(new ConnectingToBootstrapNode());
  }

  private void initializeBootstrapNodeList(String bootstrapNodes) {
    ArrayList<String> hostsAndPorts =
        Lists.newArrayList(Splitter.on(',').trimResults().omitEmptyStrings().split(bootstrapNodes));

    final int bootstrapHostCount = hostsAndPorts.size();

    if (bootstrapHostCount < 1) {
      throw new IllegalArgumentException("Need at least one bootstrap host");
    }

    _bootstrapHosts = new String[bootstrapHostCount];
    _bootstrapPorts = new int[bootstrapHostCount];

    for (int i = 0; i < bootstrapHostCount; i++) {
      String hostAndPort = hostsAndPorts.get(i);
      String[] splittedHostAndPort = hostAndPort.split(":");

      if (splittedHostAndPort.length != 2) {
        throw new IllegalArgumentException("Unable to parse host:port combination for " + hostAndPort);
      }

      _bootstrapHosts[i] = splittedHostAndPort[0];

      try {
        _bootstrapPorts[i] = Integer.parseInt(splittedHostAndPort[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Could not parse port number " + splittedHostAndPort[1] + " for host:port combination " + hostAndPort);
      }
    }
  }

  private abstract class State {
    private ConsumerState stateValue;

    protected State(ConsumerState stateValue) {
      this.stateValue = stateValue;
    }

    abstract void process();

    abstract boolean isConnectedToKafkaBroker();

    void handleConsumerException(Exception e) {
      // By default, just log the exception and switch back to CONNECTING_TO_BOOTSTRAP_NODE (which will take care of
      // closing the connection if it exists)
      LOGGER.warn("Caught Kafka consumer exception while in state {}, disconnecting and trying again for topic {}",
          _currentState.getStateValue(), _topic, e);

      Uninterruptibles.sleepUninterruptibly(250, TimeUnit.MILLISECONDS);

      setCurrentState(new ConnectingToBootstrapNode());
    }

    ConsumerState getStateValue() {
      return stateValue;
    }
  }

  private class ConnectingToBootstrapNode extends State {
    public ConnectingToBootstrapNode() {
      super(ConsumerState.CONNECTING_TO_BOOTSTRAP_NODE);
    }

    @Override
    public void process() {
      // Connect to a random bootstrap node
      if (_simpleConsumer != null) {
        try {
          _simpleConsumer.close();
        } catch (Exception e) {
          LOGGER.warn("Caught exception while closing consumer for topic {}, ignoring", _topic, e);
        }
      }

      int randomHostIndex = _random.nextInt(_bootstrapHosts.length);
      _currentHost = _bootstrapHosts[randomHostIndex];
      _currentPort = _bootstrapPorts[randomHostIndex];

      try {
        LOGGER.info("Connecting to bootstrap host {}:{} for topic {}", _currentHost, _currentPort, _topic);
        _simpleConsumer =
            new SimpleConsumer(_currentHost, _currentPort, SOCKET_TIMEOUT_MILLIS, SOCKET_BUFFER_SIZE, _clientId);
        setCurrentState(new ConnectedToBootstrapNode());
      } catch (Exception e) {
        handleConsumerException(e);
      }
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return false;
    }
  }

  private class ConnectedToBootstrapNode extends State {
    protected ConnectedToBootstrapNode() {
      super(ConsumerState.CONNECTED_TO_BOOTSTRAP_NODE);
    }

    @Override
    void process() {
      // If we're consuming from a partition, we need to find the leader so that we can consume from it. By design,
      // Kafka only allows consumption from the leader and not one of the in-sync replicas.
      setCurrentState(new FetchingLeaderInformation());
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return true;
    }
  }

  private class FetchingLeaderInformation extends State {
    public FetchingLeaderInformation() {
      super(ConsumerState.FETCHING_LEADER_INFORMATION);
    }

    @Override
    void process() {
      // Fetch leader information
      try {
        TopicMetadataResponse response =
            _simpleConsumer.send(new TopicMetadataRequest(Collections.singletonList(_topic)));
        try {
          _leader = null;
          List<PartitionMetadata> pMetaList = response.topicsMetadata().get(0).partitionsMetadata();
          for (PartitionMetadata pMeta : pMetaList) {
            if (pMeta.partitionId() == _partition) {
              _leader = new KafkaBrokerWrapper(pMeta.leader());
              break;
            }
          }

          // If we've located a broker
          if (_leader != null) {
            LOGGER.info("Located leader broker {} for topic {}, connecting to it.", _leader, _topic);
            setCurrentState(new ConnectingToPartitionLeader());
          } else {
            // Failed to get the leader broker. There could be a leader election at the moment, so retry after a little
            // bit.
            LOGGER.warn("Leader broker is null for topic {}, retrying leader fetch in 100ms", _topic);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
          }
        } catch (Exception e) {
          // Failed to get the leader broker. There could be a leader election at the moment, so retry after a little
          // bit.
          LOGGER.warn("Failed to get the leader broker for topic {} due to exception, retrying in 100ms", _topic, e);
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        handleConsumerException(e);
      }
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return true;
    }
  }

  private class ConnectingToPartitionLeader extends State {
    public ConnectingToPartitionLeader() {
      super(ConsumerState.CONNECTING_TO_PARTITION_LEADER);
    }

    @Override
    void process() {
      // If we're already connected to the leader broker, don't disconnect and reconnect
      LOGGER.info("Trying to fetch leader host and port: {}:{} for topic {}", _leader.host(), _leader.port(), _topic);
      if (_leader.host().equals(_currentHost) && _leader.port() == _currentPort) {
        setCurrentState(new ConnectedToPartitionLeader());
        return;
      }

      // Disconnect from current broker
      if (_simpleConsumer != null) {
        try {
          _simpleConsumer.close();
          _simpleConsumer = null;
        } catch (Exception e) {
          handleConsumerException(e);
          return;
        }
      }

      // Connect to the partition leader
      try {
        _simpleConsumer =
            new SimpleConsumer(_currentHost, _currentPort, SOCKET_TIMEOUT_MILLIS, SOCKET_BUFFER_SIZE, _clientId);
        setCurrentState(new ConnectedToPartitionLeader());
      } catch (Exception e) {
        handleConsumerException(e);
      }
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return false;
    }
  }

  private class ConnectedToPartitionLeader extends State {
    public ConnectedToPartitionLeader() {
      super(ConsumerState.CONNECTED_TO_PARTITION_LEADER);
    }

    @Override
    void process() {
      // Nothing to do
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return true;
    }
  }

  private void setCurrentState(State newState) {
    if (_currentState != null) {
      LOGGER.info("Switching from state {} to state {} for topic {}", _currentState.getStateValue(),
          newState.getStateValue(), _topic);
    }

    _currentState = newState;
  }

  /**
   * Fetch messages and the per-partition high watermark from Kafka between the specified offsets.
   *
   * @param startOffset The offset of the first message desired, inclusive
   * @param endOffset The offset of the last message desired, exclusive, or {@link Long#MAX_VALUE} for no end offset.
   * @param timeoutMillis Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An iterable containing messages fetched from Kafka and their offsets, as well as the high watermark for
   * this partition.
   */
  public synchronized MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException {

    final long connectEndTime = System.currentTimeMillis() + _connectionTimeoutMillis;
    while (_currentState.getStateValue() != ConsumerState.CONNECTED_TO_PARTITION_LEADER
        && System.currentTimeMillis() < connectEndTime) {
      _currentState.process();
    }
    if (_currentState.getStateValue() != ConsumerState.CONNECTED_TO_PARTITION_LEADER
        && connectEndTime <= System.currentTimeMillis()) {
      throw new java.util.concurrent.TimeoutException();
    }

    FetchResponse fetchResponse = _simpleConsumer.fetch(new FetchRequestBuilder().minBytes(100000)
        .maxWait(timeoutMillis)
        .addFetch(_topic, _partition, startOffset, 500000)
        .build());

    if (!fetchResponse.hasError()) {
      final Iterable<MessageAndOffset> messageAndOffsetIterable =
          buildOffsetFilteringIterable(fetchResponse.messageSet(_topic, _partition), startOffset, endOffset);

      return new KafkaSimpleConsumerMessageBatch(messageAndOffsetIterable);
    } else {
      throw exceptionForKafkaErrorCode(fetchResponse.errorCode(_topic, _partition));
    }
  }

  private RuntimeException exceptionForKafkaErrorCode(short kafkaErrorCode) {
    final Errors kafkaError = Errors.forCode(kafkaErrorCode);
    switch (kafkaError) {
      case UNKNOWN:
      case OFFSET_OUT_OF_RANGE:
      case CORRUPT_MESSAGE:
      case MESSAGE_TOO_LARGE:
      case OFFSET_METADATA_TOO_LARGE:
      case INVALID_TOPIC_EXCEPTION:
      case RECORD_LIST_TOO_LARGE:
      case INVALID_REQUIRED_ACKS:
      case ILLEGAL_GENERATION:
      case INCONSISTENT_GROUP_PROTOCOL:
      case INVALID_GROUP_ID:
      case UNKNOWN_MEMBER_ID:
      case INVALID_SESSION_TIMEOUT:
      case INVALID_COMMIT_OFFSET_SIZE:
        return new PermanentConsumerException(kafkaError);
      case UNKNOWN_TOPIC_OR_PARTITION:
      case LEADER_NOT_AVAILABLE:
      case NOT_LEADER_FOR_PARTITION:
      case REQUEST_TIMED_OUT:
      case BROKER_NOT_AVAILABLE:
      case REPLICA_NOT_AVAILABLE:
      case STALE_CONTROLLER_EPOCH:
      case NETWORK_EXCEPTION:
      case GROUP_LOAD_IN_PROGRESS:
      case GROUP_COORDINATOR_NOT_AVAILABLE:
      case NOT_COORDINATOR_FOR_GROUP:
      case NOT_ENOUGH_REPLICAS:
      case NOT_ENOUGH_REPLICAS_AFTER_APPEND:
      case REBALANCE_IN_PROGRESS:
      case TOPIC_AUTHORIZATION_FAILED:
      case GROUP_AUTHORIZATION_FAILED:
      case CLUSTER_AUTHORIZATION_FAILED:
        return new TransientConsumerException(kafkaError);
      case NONE:
      default:
        return new RuntimeException("Unhandled error " + kafkaError);
    }
  }

  private Iterable<MessageAndOffset> buildOffsetFilteringIterable(final ByteBufferMessageSet messageAndOffsets,
      final long startOffset, final long endOffset) {
    return Iterables.filter(messageAndOffsets, input -> {
      // Filter messages that are either null or have an offset ∉ [startOffset; endOffset[
      if (input == null || input.offset() < startOffset || (endOffset <= input.offset() && endOffset != -1)) {
        return false;
      }

      // Check the message's checksum
      // TODO We might want to have better handling of this situation, maybe try to fetch the message again?
      if (!input.message().isValid()) {
        LOGGER.warn("Discarded message with invalid checksum in partition {} of topic {}", _partition, _topic);
        return false;
      }

      return true;
    });
  }

  @Override
  public void close() throws IOException {
    boolean needToCloseConsumer = _currentState.isConnectedToKafkaBroker() && _simpleConsumer != null;

    // Reset the state machine
    setCurrentState(new ConnectingToBootstrapNode());

    // Close the consumer if needed
    if (needToCloseConsumer) {
      _simpleConsumer.close();
      _simpleConsumer = null;
    }
  }
}
