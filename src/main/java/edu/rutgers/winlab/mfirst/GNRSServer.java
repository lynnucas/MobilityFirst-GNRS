/*
 * Copyright (c) 2012, Rutgers University
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * + Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * + Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package edu.rutgers.winlab.mfirst;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thoughtworks.xstream.XStream;

import edu.rutgers.winlab.mfirst.mapping.GUIDMapper;
import edu.rutgers.winlab.mfirst.mapping.RandomSelector;
import edu.rutgers.winlab.mfirst.mapping.GeoSelector;
import edu.rutgers.winlab.mfirst.StatisticsCollector;
import edu.rutgers.winlab.mfirst.mapping.ReplicaSelector;
import edu.rutgers.winlab.mfirst.mapping.ipv4udp.IPv4UDPGUIDMapper;
import edu.rutgers.winlab.mfirst.mapping.sid.SIDGUIDMapper;
import edu.rutgers.winlab.mfirst.messages.AbstractMessage;
import edu.rutgers.winlab.mfirst.messages.AbstractResponseMessage;
import edu.rutgers.winlab.mfirst.messages.InsertMessage;
import edu.rutgers.winlab.mfirst.messages.LookupMessage;
import edu.rutgers.winlab.mfirst.net.AddressType;
import edu.rutgers.winlab.mfirst.net.MessageListener;
import edu.rutgers.winlab.mfirst.net.NetworkAccessObject;
import edu.rutgers.winlab.mfirst.net.NetworkAddress;
import edu.rutgers.winlab.mfirst.net.SessionParameters;
import edu.rutgers.winlab.mfirst.net.ipv4udp.IPv4UDPAddress;
import edu.rutgers.winlab.mfirst.net.ipv4udp.IPv4UDPNAO;
import edu.rutgers.winlab.mfirst.storage.GNRSRecord;
import edu.rutgers.winlab.mfirst.storage.GUIDBinding;
import edu.rutgers.winlab.mfirst.storage.GUIDStore;
import edu.rutgers.winlab.mfirst.storage.SimpleGUIDStore;
import edu.rutgers.winlab.mfirst.storage.bdb.BerkeleyDBStore;
import edu.rutgers.winlab.mfirst.storage.cache.CacheOrigin;
import edu.rutgers.winlab.mfirst.storage.cache.SimpleCache;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import edu.rutgers.winlab.mfirst.messages.MessageType;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Java implementation of a GNRS server.
 * 
 * @author Robert Moore
 */
public class GNRSServer implements MessageListener {

  /**
   * Default port value for GNRS servers.
   */
  public static final int DEFAULT_PORT = 5001;

  /**
   * Logging facility for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(GNRSServer.class);

  /**
   * @param args
   *          <Configuration File>
   */
  public static void main(final String[] args) {
    LOG.info("------------------------");
    LOG.info("GNRS Server starting up.");
    LOG.info("------------------------");

    if (args.length < 1) {
      LOG.error("Missing 1 or more command-line arguments.");
      printUsageInfo();

    } else {
      final XStream xStream = new XStream();
      LOG.trace("Loading configuration file \"{}\".", args[0]);
      final Configuration config = (Configuration) xStream.fromXML(new File(
          args[0]));
      LOG.debug("Finished parsing configuration file.");
      try {

        // Create the server
        final GNRSServer server = new GNRSServer(config);
        /*
         * The server bound its port and is listening, but isn't yet started.
         * Messages can arrive, but will just be queued until start() is called.
         */

        // Add a hook to capture interrupts and shut down gracefully
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            server.shutdown();
          }
        });

        LOG.debug("GNRS server object successfully created.");
        server.startup();
        LOG.trace("GNRS server thread started.");
        } catch (final IOException ioe) {
        LOG.error("Unable to start server.", ioe);

      }
    }
  }

  /**
   * Prints out a helpful message to the command line. Let the user know how to
   * invoke.
   */
  public static void printUsageInfo() {
    System.err.println("Parameters: <Config file>");
  }

  /*
   * Class stuff below here.
   */

  /**
   * Configuration file for the server.
   */
  private final transient Configuration config;

  /**
   * Whether or not to collect statistics about performance.
   */
  private final transient boolean collectStatistics;

  /**
   * Timer for various tasks.
   */
  private final transient Timer timer = new Timer();

  /**
   * Number of Lookup messages processed since last stats output.
   */
  static final AtomicInteger NUM_LOOKUPS = new AtomicInteger(0);

  /**
   * Number of Insert messages processed since last stats output.
   */
  static final AtomicInteger NUM_INSERTS = new AtomicInteger(0);
  
  /**
   * Number of Lookup forward messages processed since last stats output.
   */
  static final AtomicInteger NUM_LOOKUPS_FW = new AtomicInteger(0);

  /**
   * Number of Insert forward messages processed since last stats output.
   */
  static final AtomicInteger NUM_INSERTS_FW = new AtomicInteger(0);

  /**
   * Number of Response messages processed since last stats output.
   */
  static final AtomicInteger NUM_RESPONSES = new AtomicInteger(0);
  
  /**
   * Round-trip-time of insert requests (exclude delay induced by forwarding an insertion from client).
   */
  static final Queue<Long> insertRtts = new ConcurrentLinkedQueue<Long>();
  /**
   * Round-trip-time of lookup requests (exclude delay induced by forwarding a lookup from client).
   */
  static final Queue<Long> lookupRtts = new ConcurrentLinkedQueue<Long>();

  /**
   * time sent a lookup/insertion request (exclude from forwarding a lookup or insertion request)
   */
  static final Map<Integer, Long> sendTimes = new ConcurrentHashMap<Integer, Long>();


  /**
   * Total number of nanoseconds spent processing lookup messages since last
   * stats report.
   */
  static final AtomicLong[] LOOKUP_STATS = new AtomicLong[5];

  /**
   * Total number of nanoseconds spent processing insert messages since last
   * stats report.
   */
  static final AtomicLong[] INSERT_STATS = new AtomicLong[5];

  static final transient int QUEUE_TIME_INDEX = 0;

  static final transient int PROC_TIME_INDEX = 1;

  static final transient int REMOTE_TIME_INDEX = 2;

  static final transient int RESP_PROC_TIME_INDEX = 3;

  static final transient int TOTAL_TIME_INDEX = 4;

  static {
    LOOKUP_STATS[QUEUE_TIME_INDEX] = new AtomicLong(0);
    LOOKUP_STATS[PROC_TIME_INDEX] = new AtomicLong(0);
    LOOKUP_STATS[REMOTE_TIME_INDEX] = new AtomicLong(0);
    LOOKUP_STATS[RESP_PROC_TIME_INDEX] = new AtomicLong(0);
    LOOKUP_STATS[TOTAL_TIME_INDEX] = new AtomicLong(0);

    INSERT_STATS[QUEUE_TIME_INDEX] = new AtomicLong(0);
    INSERT_STATS[PROC_TIME_INDEX] = new AtomicLong(0);
    INSERT_STATS[REMOTE_TIME_INDEX] = new AtomicLong(0);
    INSERT_STATS[RESP_PROC_TIME_INDEX] = new AtomicLong(0);
    INSERT_STATS[TOTAL_TIME_INDEX] = new AtomicLong(0);
  }

  /**
   * Thread pool for distributing tasks.
   */
  private final transient ExecutorService workers;

  /**
   * Networking interface for the server.
   */
  private final transient NetworkAccessObject networkAccess;

  /**
   * Mapping provider for converting a GUID to a set of Network Address values.
   */
  private final transient GUIDMapper guidMapper;

  /**
   * GUID binding storage object.
   */
  private final transient GUIDStore guidStore;

  /**
   * Messages sent to other servers for processing. These messages are awaiting
   * a response from a remote server.
   */
  final transient Map<Integer, RelayInfo> awaitingResponse = new ConcurrentHashMap<Integer, RelayInfo>();

  /**
   * Unique message ID values for this server.
   */
  private final transient AtomicInteger nextRequestId = new AtomicInteger(
      (int) System.currentTimeMillis());

  /**
   * Cache for storing values destined for other systems.
   */
  private final transient SimpleCache cache;

  /**
   * Selector for which of the k replicas to contact for lookups.
   */
  private final transient ReplicaSelector replicaSelector;

  /**
   * Creates a new GNRS server with the specified configuration. The server will
   * not start running until the {@link #startup()} method is invoked.
   * 
   * @param config
   *          the configuration to use.
   * @throws IOException
   *           if an IOException occurs during server set-up.
   */
  public GNRSServer(final Configuration config) throws IOException {
    super();
    this.config = config;
    this.collectStatistics = this.config.isCollectStatistics();

    // Configure extra threads to handle message processing
    int numThreads = this.config.getNumWorkerThreads();
    if (numThreads < 1) {
      numThreads = 1;
    }

    StatisticsCollector.setPath(this.config.getStatsDirectory());

    // LOG.info("Using threadpool of {} threads.", Integer.valueOf(numThreads));
    this.workers = Executors.newFixedThreadPool(numThreads);

    this.networkAccess = createNAO(this.config);
    if (this.networkAccess == null) {
      LOG.error("Unable to create network access of type {}",
          this.config.getNetworkType());
      throw new IllegalArgumentException(
          "Unable to create network access object.");
    }

    this.guidMapper = createMapper(this.config);
    if (this.guidMapper == null) {

      LOG.error("Unable to create GUID mapper of type {}",
          this.config.getNetworkType());
      throw new IllegalArgumentException("Unable to create GUID mapper object.");
    }
    this.guidStore = this.createStore(this.config);
    if (this.guidStore == null) {
      LOG.error("Unable to create GUID store of type {}",
          this.config.getStoreType());
      throw new IllegalArgumentException("Unable to create GUID store object.");
    }

    if (this.config.getCacheEntries() > 0) {
      this.cache = new SimpleCache(this.config.getCacheEntries());
    } else {
      this.cache = null;
    }

    this.replicaSelector = this.createRepSelector(this.config);
    if (this.replicaSelector == null) {
      LOG.error("Unable to create replica selector type {}",
          this.config.getReplicaSelector());
      throw new IllegalArgumentException(
          "Unable to create replica selector object.");
    }

    this.networkAccess.addMessageListener(this);

  }

  /**
   * Starts any necessary threads.
   * 
   * @return {@code true} if everything starts correctly.
   */

  public boolean startup() {
      
    if (this.collectStatistics) {
      this.timer.scheduleAtFixedRate(new StatsTask(this), 4000, 4000);
      //this.timer.scheduleAtFixedRate(new StatsTask(this), 1000, 1000);
    }
    long sweepTime = this.config.getTimeoutMillis() / 4;
    if (sweepTime < 250) {
      sweepTime = 250;
    } else if (sweepTime > 5000) {
      sweepTime = 5000;
    }
    LOG.info("sweepTime {}",sweepTime);
    sweepTime = 2500;
    this.timer.scheduleAtFixedRate(
        new ResponseSweepTask(this.config.getTimeoutMillis(),
            this.awaitingResponse, this.workers, this), sweepTime, sweepTime);

    this.guidStore.doInit();
    return true;
  }

  /**
   * Configures the server to use a specific type of networking.
   * 
   * @param config
   *          * the server configuration.
   * @return {@code true} if configuration succeeds, else {@code false}.
   */
  // TODO: Add new network types here.
  private static NetworkAccessObject createNAO(final Configuration config) {
    NetworkAccessObject netAccess = null;
    // IPv4 + UDP
    if ("ipv4udp".equalsIgnoreCase(config.getNetworkType())) {
      try {
        netAccess = new IPv4UDPNAO(config.getNetworkConfiguration());
      } catch (final IOException ioe) {
        LOG.error("Unable to create IPv4/UDP network access.", ioe);
      }
    } else {
      LOG.error("Unrecognized networking type: {}", config.getNetworkType());
    }
    return netAccess;
  }

  /**
   * Initializes the GUID&rarr;NetworkAddress mapper for the specified network
   * type.
   * 
   * @param config
   *          the server configuration.
   * @return {@code true} if the binding was successful, else {@code false}.
   * Revised by Yi: removed Static modifier to the function due to the need to pass the local address
   * to the SIDGUIDMapper to sort the replicas based on routing distance
   */
  private  GUIDMapper createMapper(final Configuration config) {
    GUIDMapper mapper = null;
    // IPv4 + UDP Revised by: Yi Hu
    if ("ipv4udp".equalsIgnoreCase(config.getMappingType())) {
      try {
        mapper = new IPv4UDPGUIDMapper(config.getMappingConfiguration());
      } catch (final IOException ioe) {
        LOG.error("Unable to create IPv4/UDP GUID mapper.", ioe);
      }
    } else if ("sid".equalsIgnoreCase(config.getMappingType())) {
      try {
        LOG.info("Begin to create SID GUID mapper.");
        InetSocketAddress localSocketAddress;
          if (!(getOriginAddress()== null)) {
              localSocketAddress = IPv4UDPAddress.toSocketAddr(getOriginAddress());
              mapper = new SIDGUIDMapper(config.getMappingConfiguration(), 
                IPv4UDPAddress.toSocketAddr(getOriginAddress()));
          } else {
              LOG.error("Unable to create SID GUID mapper due to null local InetSocketAddress.");
          }
        LOG.info("Finish creating SID GUID mapper.");
      } catch (final IOException ioe) {
        LOG.error("Unable to create SID GUID mapper.", ioe);
      }
    }else {
      LOG.error("Unrecognized networking type: {}", config.getNetworkType());
    }
    return mapper;

  }

  /**
   * Creates the GUID data store based on the configuration parameters.
   * 
   * @param config
   *          the server configuration.
   * @return a new GUID data store, or {@code null} if an error occurred.
   */
  private GUIDStore createStore(final Configuration config) {
    GUIDStore store = null;
    if ("berkeleydb".equalsIgnoreCase(config.getStoreType())) {
      store = new BerkeleyDBStore(config.getStoreConfiguration(), this);
    } else if ("simple".equalsIgnoreCase(config.getStoreType())) {
      store = new SimpleGUIDStore();
    } else {
      LOG.error("Unrecognized store type: {}", config.getStoreType());
    }
    return store;
  }

  /**
   * Creates a Replica Selector based on the configuration parameters.
   * 
   * @param config
   *          the server configuration.
   * @return a new ReplicaSelector, or {@code null} if an error occurred.
   */
  private ReplicaSelector createRepSelector(final Configuration config) {
    ReplicaSelector selector = null;
    if ("random".equalsIgnoreCase(config.getReplicaSelector())) {
      selector = new RandomSelector(2);
    } else if ("geo".equalsIgnoreCase(config.getReplicaSelector())) {
      selector = new GeoSelector(3);
    }else {
      LOG.error("Unrecognized replica selector type: {}",
          config.getReplicaSelector());
    }
    return selector;
  }

  /**
   * Terminates the server in a graceful way.
   */
  public void shutdown() {

    this.guidStore.doShutdown();

    this.timer.cancel();
    this.workers.shutdown();

    this.outputCDF();
  }

  private void outputCDF() {
    LOG.info("Generating CDF files.");
    StatisticsCollector.toFiles();
  }

  /**
   * Returns the set of bindings for a GUID value.
   * 
   * @param guid
   *          the GUID value to get bindings for.
   * @return the current binding values.
   */
  public GUIDBinding[] getBindings(final GUID guid) {
    GUIDBinding[] addresses = null;
    final GNRSRecord record = this.guidStore.getBindings(guid);
    if (record != null) {
      addresses = record.getBindings();
    }

    return addresses;
  }

  /**
   * Convenience method for Tasks to insert GUID bindings.
   * 
   * @param guid
   *          the GUID to bind.
   * @param addresses
   *          the new bindings for the GUID
   * @return {@code true} if the insert succeeds, else {@code false}.
   */
  public boolean appendBindings(final GUID guid,
      final NetworkAddress... addresses) {

    GUIDBinding binding = null;
    for (final NetworkAddress a : addresses) {
      binding = new GUIDBinding();
      binding.setAddress(a);
      binding.setTtl(0);
      binding.setWeight(0);
      this.guidStore.appendBindings(guid, binding);
    }
    return true;
  }

  /**
   * Convenience accessor method for tasks to retrieve the correct mapping for a
   * GUID.
   * 
   * @param guid
   *          the GUID to map.
   * @param types
   *          the set of AddressTypes to create mappings for. If no types are
   *          specified, then the server's default address type is created.
   * @return a Collection containing the appropriate network mappings, or
   *         {@code null} if none could be created.
   */
  public Collection<NetworkAddress> getMappings(final GUID guid,
      final AddressType... types) {
    return this.guidMapper
        .getMapping(guid, this.config.getNumReplicas(), types);
  }

  /*
  * retrieve the network address of the server to which the request is forwarded to
  * the server is selected to be with the same geo-location as the guid
  * @param guid 
  *         the GUID a insertion/lookup request is targeted
  * @return the network address of the server this request should be forwarded to
  */
  public NetworkAddress forwardToServer (final GUID guid){
      return this.guidMapper
         .getSIDAddrbyLocation(guid.getCity(), guid.getState(), guid.getCountry());
  }
  
  /**
   * Returns a List of NetworkAddresses based on the replica selector policy
   * defined
   * in the configuration of this server.
   * 
   * @param replicas
   *          the total set of possible servers.
   * @return a List of replicas to contact, ordered from most to least
   *         preferred.
   */
  public List<NetworkAddress> getPreferredReplicas(
      final Collection<NetworkAddress> replicas) {
    return this.replicaSelector.getContactList(replicas);
  }

  /**
   * Sends the specified message to the destination addresses, if possible.
   * Actual send of the message may be delayed or aborted if, for instance the
   * remote addresses are temporarily unreachable, of a type the server cannot
   * handle,
   * or if networking is disabled or non-functional.
   * 
   * @param message
   *          the message to send.
   * @param destAddrs
   *          the recipient addresses.
   message send zz
   */
  public void sendMessage(final AbstractMessage message,
      final NetworkAddress... destAddrs) {
    this.networkAccess.sendMessage(message, destAddrs);
  }

  /**
   * Convenience method for tasks to check if a NetworkAddress references the
   * local server or not.
   * 
   * @param address
   *          the address to check
   * @return {@code true} if the address identifies this server, else
   *         {@code false}.
   * @see NetworkAccessObject#isLocal(NetworkAddress)
   */
  public boolean isLocalAddress(final NetworkAddress address) {
    return this.networkAccess.isLocal(address);
  }

  /**
   * Gets this server's origin address. The type is dependent on the NAO being
   * used.
   * 
   * @return the origin address value for this server.
   */
  public NetworkAddress getOriginAddress() {
    return this.networkAccess.getOriginAddress();
  }

  @Override
  public void messageReceived(final SessionParameters parameters,
      final AbstractMessage msg) {

    //LOG.info("Received {}.", msg); zz got three categories of messages.
    if (msg instanceof InsertMessage) {
        this.workers
          .submit(new InsertTask(this, parameters, (InsertMessage) msg));

    } else if (msg instanceof LookupMessage) {
        this.workers
          .submit(new LookupTask(this, parameters, (LookupMessage) msg));
    } else if (msg instanceof AbstractResponseMessage) {
        this.workers.submit(new ResponseTask(this, parameters,
          (AbstractResponseMessage) msg));
    }
    // Unrecognized or invalid message received
    else {
      LOG.warn("Unrecognized message: {}", msg);
      this.networkAccess.endSession(parameters);
    }

  }

  /**
   * Marks a server as necessary for a specific request ID
   * 
   * @param requestId
   *          the request ID of the message sent to the server
   * @param info
   *          the server/message information.
   */
  public void addNeededServer(final Integer requestId, final RelayInfo info) {
    this.awaitingResponse.put(Integer.valueOf(requestId), info);
  }

  /**
   * Get the next available request ID.
   * 
   * @return
   */
  public int getNextRequestId() {
    return this.nextRequestId.getAndIncrement();
  }

  /**
   * Accesses the server's cache to check for bindings for the specified GUID
   * value.
   * 
   * @param guid
   *          the GUID to retrieve
   * @return the cached bindings, if any exist in the cache.
   */
  public Collection<GUIDBinding> getCached(final GUID guid) {

    return this.cache == null ? null : this.cache.get(guid);
  }

  /**
   * Inserts the specified NetworkAddress values into the server's local cache.
   * 
   * @param guid
   *          the GUID for the bindings.
   * @param bindings
   *          the addresses to bind.
   */
  public void addToCache(final GUID guid, final CacheOrigin origin,
      final GUIDBinding... bindings) {
    if (this.cache != null) {
      this.cache.put(guid, origin, bindings);
    }
  }

  /**
   * Timer task that reports server-related statistics when called.
   * 
   * @author Robert Moore
   */
  public static final class StatsTask extends TimerTask {

    private final transient GNRSServer server;

    public StatsTask(final GNRSServer server) {
      super();
      this.server = server;
    }

    /**
     * Logging for the statistics class.
     */
    private static final Logger LOG_STATS = LoggerFactory
        .getLogger(StatsTask.class);

    /**
     * The last time statistics were generated.
     */
    private transient long lastTimestamp = System.currentTimeMillis();

    @Override
    public void run() {
      final long now = System.currentTimeMillis();

      final long lkpQueueNanos = GNRSServer.LOOKUP_STATS[QUEUE_TIME_INDEX]
          .getAndSet(0);
      final long lkpProcNanos = GNRSServer.LOOKUP_STATS[PROC_TIME_INDEX]
          .getAndSet(0);
      final long lkpRemNanos = GNRSServer.LOOKUP_STATS[REMOTE_TIME_INDEX]
          .getAndSet(0);
      final long lkpRespNanos = GNRSServer.LOOKUP_STATS[RESP_PROC_TIME_INDEX]
          .getAndSet(0);
      final long lkpTotalNanos = GNRSServer.LOOKUP_STATS[TOTAL_TIME_INDEX]
          .getAndSet(0);

      final long insQueueNanos = GNRSServer.INSERT_STATS[QUEUE_TIME_INDEX]
          .getAndSet(0);
      final long insProcNanos = GNRSServer.INSERT_STATS[PROC_TIME_INDEX]
          .getAndSet(0);
      final long insRemNanos = GNRSServer.INSERT_STATS[REMOTE_TIME_INDEX]
          .getAndSet(0);
      final long insRespNanos = GNRSServer.INSERT_STATS[RESP_PROC_TIME_INDEX]
          .getAndSet(0);
      final long insTotalNanos = GNRSServer.INSERT_STATS[TOTAL_TIME_INDEX]
          .getAndSet(0);

      final int numLookups = GNRSServer.NUM_LOOKUPS.getAndSet(0);
      final int numInserts = GNRSServer.NUM_INSERTS.getAndSet(0);
      final int numLookupFws = GNRSServer.NUM_LOOKUPS_FW.getAndSet(0);
      final int numInsertFws = GNRSServer.NUM_INSERTS_FW.getAndSet(0);
      final int numResponse = GNRSServer.NUM_RESPONSES.getAndSet(0);

      final long timeDiff = now - this.lastTimestamp;
      this.lastTimestamp = now;
      final float numSeconds = timeDiff / 1000f;
      final float lookupsPerSecond = numLookups / numSeconds;
      final float insertsPerSecond = numInserts / numSeconds;
      final float responsesPerSecond = numResponse / numSeconds;

      final float insQueueAvg = numInserts == 0 ? 0
          : (insQueueNanos / numInserts) / 1000f;
      final float insProcAvg = numInserts == 0 ? 0
          : (insProcNanos / numInserts) / 1000f;
      final float insRemAvg = numInserts == 0 ? 0
          : (insRemNanos / numInserts) / 1000f;
      final float insRespAvg = numInserts == 0 ? 0
          : (insRespNanos / numInserts) / 1000f;
      final float insTotAvg = numInserts == 0 ? 0
          : (insTotalNanos / numInserts) / 1000f;

      final float lkpQueueAvg = numLookups == 0 ? 0
          : (lkpQueueNanos / numLookups) / 1000f;
      final float lkpProcAvg = numLookups == 0 ? 0
          : (lkpProcNanos / numLookups) / 1000f;
      final float lkpRemAvg = numLookups == 0 ? 0
          : (lkpRemNanos / numLookups) / 1000f;
      final float lkpRespAvg = numLookups == 0 ? 0
          : (lkpRespNanos / numLookups) / 1000f;
      final float lkpTotAvg = numLookups == 0 ? 0
          : (lkpTotalNanos / numLookups) / 1000f;

      if (lkpTotAvg > 0) {
        StatisticsCollector.addValue("lkp-queue", lkpQueueAvg);
        StatisticsCollector.addValue("lkp-proc", lkpProcAvg);
        StatisticsCollector.addValue("lkp-remote", lkpRemAvg);
        StatisticsCollector.addValue("lkp-resp", lkpRespAvg);
        StatisticsCollector.addValue("lkp-total", lkpTotAvg);
      }

      if (insTotAvg > 0) {
        StatisticsCollector.addValue("ins-queue", insQueueAvg);
        StatisticsCollector.addValue("ins-proc", insProcAvg);
        StatisticsCollector.addValue("ins-remote", insRemAvg);
        StatisticsCollector.addValue("ins-resp", insRespAvg);
        StatisticsCollector.addValue("ins-total", insTotAvg);
      }

      String statsFormatString = "\n==Lookups==\n"
          + "%.3f per second (Q: %,.1f us, P: %,.1f us, R: %,.1f us, S: %,.1f us, T: %,.1f us)\n"
          + "==Inserts==\n"
          + "%.3f per second (Q: %,.1f us, P: %,.1f us, R: %,.1f us, S: %,.1f us, T: %,.1f us)\n"
          + "Outstanding responses: %,d\n";

      LOG_STATS.info(String.format(statsFormatString,
          Float.valueOf(lookupsPerSecond), Float.valueOf(lkpQueueAvg),
          Float.valueOf(lkpProcAvg), Float.valueOf(lkpRemAvg),
          Float.valueOf(lkpRespAvg), Float.valueOf(lkpTotAvg),
          Float.valueOf(insertsPerSecond), Float.valueOf(insQueueAvg),
          Float.valueOf(insProcAvg), Float.valueOf(insRemAvg),
          Float.valueOf(insRespAvg), Float.valueOf(insTotAvg),
          Integer.valueOf(this.server.awaitingResponse.size())));
      
    int length = GNRSServer.insertRtts.size();
    ArrayList<Long> insList = new ArrayList<Long>(length);
    insList.addAll(GNRSServer.insertRtts);
    Collections.sort(insList);

    long minInsRtt = insList.isEmpty() ? 0 : insList.get(0);
    long medInsRtt = insList.isEmpty() ? 0 : insList.get(length / 2);
    long maxInsRtt = insList.isEmpty() ? 0 : insList.get(length - 1);
    minInsRtt /= 1000;
    medInsRtt /= 1000;
    maxInsRtt /= 1000;
    
    length = GNRSServer.lookupRtts.size();
    ArrayList<Long> lookList = new ArrayList<Long>(length);
    lookList.addAll(GNRSServer.lookupRtts);
    Collections.sort(lookList);

    long minLkpRtt = lookList.isEmpty() ? 0 : lookList.get(0);
    long medLkpRtt = lookList.isEmpty() ? 0 : lookList.get(length / 2);
    long maxLkpRtt = lookList.isEmpty() ? 0 : lookList.get(length - 1);

    minLkpRtt /= 1000;
    medLkpRtt /= 1000;
    maxLkpRtt /= 1000;
      String statsFormatString2 = "\n==Lookup Forwards==%d ==Lookups== %d%n"
              + "Min: %,dus | Med: %,dus | Max: %,dus\n"
              +"\n==Insertion Forwards==%d ==Insertions== %d%n"
              + "Min: %,dus | Med: %,dus | Max: %,dus\n";
      LOG_STATS.info(String.format(statsFormatString2, 
              Integer.valueOf(numLookupFws), Integer.valueOf(numLookups),
              Long.valueOf(minLkpRtt),Long.valueOf(medLkpRtt), Long.valueOf(maxLkpRtt),
              Integer.valueOf(numInsertFws), Integer.valueOf(numInserts),
              Long.valueOf(minInsRtt),Long.valueOf(medInsRtt), Long.valueOf(maxInsRtt)));
    }
  }

  /**
   * Simple task for sweeping the responses and clearing-out those that have
   * expired.
   * 
   * @author Robert Moore
   */
  private static final class ResponseSweepTask extends TimerTask {

    private final transient Map<Integer, RelayInfo> infoCollection;
    private final transient ExecutorService workers;
    private final transient long maxAge;
    private final transient GNRSServer server;

    /**
     * Creates a new task to monitor/sweep the specified collection. Any failed
     * info will generate a new TimeoutTask which will be sent to the
     * ExecutorService provided.
     * 
     * @param maxAge
     *          the timeout period for a request.
     * @param monitoredCollection
     *          the objects to monitor
     * @param workers
     *          workers for handling the timeouts
     */
    public ResponseSweepTask(final long maxAge,
        final Map<Integer, RelayInfo> monitoredCollection,
        final ExecutorService workers, final GNRSServer server) {
      super();
      this.infoCollection = monitoredCollection;
      this.workers = workers;
      this.maxAge = maxAge;
      this.server = server;
    }

    @Override
    public void run() {

      final long cutoff = System.currentTimeMillis() - this.maxAge;
      for (final Iterator<Integer> iter = this.infoCollection.keySet()
          .iterator(); iter.hasNext();) {
        Integer reqId = iter.next();
        RelayInfo info = this.infoCollection.get(reqId);
        if (info != null) {
          if (info.getAttemptTs() < cutoff) {
            info = this.infoCollection.remove(reqId);
            // Sanity/concurrency check
            if (info != null) {
              this.workers.submit(new TimeoutTask(this.server, info));
            }
          }
        }
      }

    }
  }

  /**
   * Returns a reference to this server's statistics Timer object. If statistics
   * are not enabled, then the timer will be {@code null}.
   * 
   * @return this server's Timer object for statistics reporting.
   */
  public Timer getTimer() {
    return this.timer;
  }

  /**
   * Gets this server's configuration.
   * 
   * @return this server's configuration.
   */
  public Configuration getConfig() {
    return this.config;
  }

  public boolean isCollectStatistics() {
    return collectStatistics;
  }
  //modified by yi
  public static boolean collectRtt(int requestID, long endTime,  MessageType msgType){
      Long startTime = GNRSServer.sendTimes.remove(Integer.valueOf(requestID));
      if (startTime != null) {
        long rtt = endTime - startTime.longValue();
          if (msgType == MessageType.INSERT) {
              GNRSServer.insertRtts.add(Long.valueOf(rtt));
              StatisticsCollector.addValue("svr-ins-rtt", rtt / 1000f);
              LOG.info("collect a insert rtt {}", rtt);
              return true;
          } else if (msgType == MessageType.LOOKUP) {
              GNRSServer.lookupRtts.add(Long.valueOf(rtt));
              StatisticsCollector.addValue("srv-lkp-rtt", rtt / 1000f);
              LOG.info("collect a lookup rtt {}", rtt);
              return true;
          }
      }
      return false;
  }

}
