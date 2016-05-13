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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.rutgers.winlab.mfirst.messages.InsertMessage;
import edu.rutgers.winlab.mfirst.messages.InsertResponseMessage;
import edu.rutgers.winlab.mfirst.messages.LookupMessage;
import edu.rutgers.winlab.mfirst.messages.LookupResponseMessage;
import edu.rutgers.winlab.mfirst.messages.MessageType;
import edu.rutgers.winlab.mfirst.messages.ResponseCode;
import edu.rutgers.winlab.mfirst.messages.opt.ExpirationOption;
import edu.rutgers.winlab.mfirst.messages.opt.Option;
import edu.rutgers.winlab.mfirst.messages.opt.RecursiveRequestOption;
import edu.rutgers.winlab.mfirst.messages.opt.TTLOption;
import edu.rutgers.winlab.mfirst.net.NetworkAddress;
import edu.rutgers.winlab.mfirst.net.SessionParameters;
import edu.rutgers.winlab.mfirst.net.ipv4udp.IPv4UDPAddress;
import edu.rutgers.winlab.mfirst.storage.GUIDBinding;
import edu.rutgers.winlab.mfirst.storage.cache.CacheOrigin;

/**
 * Task to handle Insert messages within the server. Designed to operate
 * independently of any other messages.
 * 
 * @author Robert Moore
 */
public class InsertTask implements Callable<Object> {

  /**
   * Logging for this class.
   */
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(InsertTask.class);

  /**
   * The message and some metadata for this task.
   */
  private final transient SessionParameters params;

  /**
   * The received insert message.
   */
  private final transient InsertMessage message;

  /**
   * The server that is handling the message.
   */
  private final transient GNRSServer server;

  /**
   * Creates a new InsertTask for the specified server and message container.
   * 
   * @param server
   *          the server that received or is handling the message.
   * @param params
   *          the session-related metadata.
   * @param message
   *          the message to process
   */
  public InsertTask(final GNRSServer server, final SessionParameters params,
      final InsertMessage message) {
    super();
    this.server = server;
    this.params = params;
    this.message = message;
  }

  @Override
  public Object call() {
    final long startProc = System.nanoTime();
    //Original code: GNRSServer.NUM_INSERTS.incrementAndGet();
    // check the insertion request is a forward insertion request or not 
    if (this.message.getGuid().getRequestType() == MessageType.FORWARD_INSERT) {
        GNRSServer.NUM_INSERTS_FW.incrementAndGet();
        // forward the insertion to a geo-compatible server
          NetworkAddress forwardToServer = this.server.forwardToServer(this.message.getGuid());
          if (forwardToServer != null) {
              
              final RelayInfo info = new RelayInfo();
              info.clientMessage = this.message;
              info.remainingServers.add(forwardToServer);
              //test
              InsertMessage testMsg = (InsertMessage)info.clientMessage;
              //IPv4UDPAddress fromAddr = (IPv4UDPAddress)info.clientMessage.getOriginAddress();
              LOG.info("Recieved an forward_insertion {}", info.clientMessage.getRequestId());
              final int requestId = this.server.getNextRequestId();

              final InsertMessage fwdInsMessage = new InsertMessage();
              GUID ins_guid = GUID.copyGUID(this.message.getGuid());
              ins_guid.setRequestType(MessageType.INSERT);
              fwdInsMessage.setGuid(ins_guid);
              
              for (Option opt : this.message.getOptions()) {
                  fwdInsMessage.addOption(opt);
              }
              fwdInsMessage.finalizeOptions();

              fwdInsMessage.setOriginAddress(this.server.getOriginAddress());
              fwdInsMessage.setVersion((byte) 0);
              fwdInsMessage.setRequestId(requestId);
              fwdInsMessage.setBindings(this.message.getBindings());
              this.server.addNeededServer(Integer.valueOf(requestId), info);
              this.server.sendMessage(fwdInsMessage, forwardToServer);
              if (this.server.getConfig().isCollectStatistics()) {
                //to do
              }
              info.markAttempt();
              LOG.info("forwarded an insertion {}, {}", this.message.getGuid().toString(), fwdInsMessage.getRequestId());
              //test
              LOG.info("testMsg guid requestType {}, changed to {}", testMsg.getGuid().getRequestType()
              , fwdInsMessage.getGuid().getRequestType());
        } else {
              LOG.error("Fail to get Forward Server in Insertion");
        }
        return null;
    }
    //handle insertion request
    GNRSServer.NUM_INSERTS.incrementAndGet();
    LOG.info("Received an insertion {}, {}", this.message.getRequestId(), this.message.getGuid().toString());
    //IPv4UDPAddress fromAddr = (IPv4UDPAddress)this.message.getOriginAddress();
    //LOG.info("from {}", fromAddr);
    final Collection<NetworkAddress> serverAddxes = this.server.getMappings(
        this.message.getGuid(), this.message.getOriginAddress().getType());
    // Determine if the local server should also handle this insert
    boolean resolvedLocally = false;
    if (serverAddxes != null && !serverAddxes.isEmpty()) {
      for (Iterator<NetworkAddress> iter = serverAddxes.iterator(); iter
          .hasNext();) {
        NetworkAddress addx = iter.next();
        // Loopback? Then the local server should handle it.
        if (this.server.isLocalAddress(addx)) {
          iter.remove();
          resolvedLocally = true;
        }
      }
    }
    boolean localSuccess = false;

    boolean recursive = false;
    List<Option> options = this.message.getOptions();
    long[] expirationTimes = null;
    long[] ttlValues = null;
    if (!options.isEmpty()) {
      for (Option opt : options) {
        if (opt instanceof RecursiveRequestOption) {
          recursive = ((RecursiveRequestOption) opt).isRecursive();
        } else if (opt instanceof ExpirationOption) {
          expirationTimes = ((ExpirationOption) opt).getExpiration();
        } else if (opt instanceof TTLOption) {
          ttlValues = ((TTLOption) opt).getTtl();
        }
      }
    }
    boolean originalRecursive = recursive;
     //record send time of an initial insert request
      long sendTime = System.nanoTime();
      if (recursive) {
          GNRSServer.sendTimes.put(Integer.valueOf((int) this.message.getRequestId()),
            Long.valueOf(sendTime));
          LOG.info("Insert record a send time {}, {}", this.message.getRequestId(), sendTime);
      }
      long endTime;
      Long startTime;
    LOG.info("Insertion recursive {}, {}.", recursive, this.message.getRequestId());
    // Insert to local server if mapped to it.
    if (resolvedLocally) {
      LOG.info("Resolved locally Insertion {}.", this.message.getRequestId());
      localSuccess = this.server.appendBindings(this.message.getGuid(),
          this.message.getBindings());
    }
    // Insert into the cache if the insert came from a local client.
    if (!resolvedLocally && recursive && this.message.getBindings() != null) {
      final long now = System.currentTimeMillis();
      final long defaultTtl = now + this.server.getConfig().getDefaultTtl();
      final long defaultExpire = now + this.server.getConfig().getDefaultExpiration();
      GUIDBinding[] bindings = new GUIDBinding[this.message.getBindings().length];

      for (int i = 0; i < bindings.length; ++i) {
        NetworkAddress netAddr = this.message.getBindings()[i];
        GUIDBinding bind = new GUIDBinding();
        bind.setAddress(netAddr);
        if (expirationTimes != null) {
          bind.setExpiration(expirationTimes[i]);
        }else{
          bind.setExpiration(defaultExpire);
        }
        if (ttlValues != null) {
          bind.setTtl(now+ttlValues[i]);
        }else{
          bind.setTtl(defaultTtl);
        }
        bindings[i] = bind;
      }

      this.server.addToCache(this.message.getGuid(), CacheOrigin.INSERT,bindings);
    }

    // Corner case for only a single server.
    if (resolvedLocally && serverAddxes.isEmpty()) {
      recursive = false;
    }
    LOG.info("Insertion recursive {}, {}", recursive, this.message.getRequestId());
    // Now send to the remote servers
    if (recursive) {
      // this.message.setRecursive(false);

      final RelayInfo info = new RelayInfo();
      info.clientMessage = this.message;
      info.remainingServers.addAll(serverAddxes);

      final int requestId = this.server.getNextRequestId();

      final InsertMessage relayMessage = new InsertMessage();
      relayMessage.setGuid(this.message.getGuid());

      for (Option opt : this.message.getOptions()) {
        if (!(opt instanceof RecursiveRequestOption)) {
          relayMessage.addOption(opt);
        }
      }
      relayMessage.finalizeOptions();

      relayMessage.setOriginAddress(this.server.getOriginAddress());
      relayMessage.setVersion((byte) 0);
      relayMessage.setRequestId(requestId);
      relayMessage.setBindings(this.message.getBindings());

      if (serverAddxes != null) {
        this.server.addNeededServer(Integer.valueOf(requestId), info);
        this.server.sendMessage(relayMessage,
            serverAddxes.toArray(new NetworkAddress[] {}));
        if (this.server.getConfig().isCollectStatistics()) {
          long endProc = System.nanoTime();
          this.message.processingNanos = endProc-startProc;
          this.message.queueNanos = startProc-this.message.createdNanos;
          this.message.forwardNanos = endProc;
        }
        info.markAttempt();
        LOG.info("Send Insertion relay {}, {}", this.message.getGuid().toString(), serverAddxes.size());
      } else {
        LOG.error("Invalid server addresses.  Cannot forward.");
      }
    }

    // Send a response back if there are no remote servers contacted
    if (resolvedLocally && !recursive) {

      InsertResponseMessage response = new InsertResponseMessage();
      response.setOriginAddress(this.server.getOriginAddress());
      response.setRequestId(this.message.getRequestId());
      response.setResponseCode(localSuccess ? ResponseCode.SUCCESS
          : ResponseCode.FAILED);
      response.setVersion((byte) 0);
      //record a rtt by resolved locally an initial insert request
        if (originalRecursive) {
            endTime = System.nanoTime();
          if (GNRSServer.collectRtt((int) this.message.getRequestId(), endTime, MessageType.INSERT)) {
                LOG.info("Add an insert Rtt by resolved locally as a single server {}",this.message.getRequestId());
           }
        }
      
      this.server.sendMessage(response, this.message.getOriginAddress());
      LOG.info("Send Insertion response {},{}", this.message.getGuid().toString(), response.getRequestId());
      
    }
    
    // Statistics
    
    if (this.server.getConfig().isCollectStatistics()) {
      long endProc = System.nanoTime();
      GNRSServer.INSERT_STATS[GNRSServer.QUEUE_TIME_INDEX].addAndGet(startProc
          - this.message.createdNanos);
      GNRSServer.INSERT_STATS[GNRSServer.PROC_TIME_INDEX].addAndGet(endProc
          - startProc);
      GNRSServer.INSERT_STATS[GNRSServer.TOTAL_TIME_INDEX].addAndGet(endProc
          - this.message.createdNanos);

    }

    return null;
  }

}
