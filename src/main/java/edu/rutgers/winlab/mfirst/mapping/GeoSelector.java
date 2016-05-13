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
package edu.rutgers.winlab.mfirst.mapping;

import edu.rutgers.winlab.mfirst.SID;
import java.util.Collection;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;
import edu.rutgers.winlab.mfirst.net.NetworkAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author Robert Moore
 */
public class GeoSelector implements ReplicaSelector {
  
  /**
   * The number of servers to return.
   */
  private final transient int numSelected;
  
  
  /**
   * Logging for this class.
   */
  private static final Logger LOG = LoggerFactory
      .getLogger(GeoSelector.class);

  /**
   * Convenience constructor that uses the supplied number of servers to
   * select and the current system time as the random seed.
   * 
   * @param numSelected
   *          the maximum number of servers to return.
   *          At least 3 replicas should be selected, 1 local, 1 regional, 1 global
   */
  public GeoSelector(int numSelected){
    this.numSelected = numSelected>=3?numSelected:3;
  }
 

  /**
   * Selects a set of the NetworkAddresses by sequentially eliminating last NetworkAddress
   * values from the original Collection (sorted by routing distance ascending order 
   * at SIDGUIDmapper)
   */
  @Override
  public List<NetworkAddress> getContactList(
      final Collection<NetworkAddress> servers) {
    final List<NetworkAddress> returnedList = new LinkedList<NetworkAddress>();
    returnedList.addAll(servers);
    final int max = Math.min(this.numSelected, servers.size());
    while (returnedList.size() > max) {
      // Remove an element from rear
      returnedList.remove(returnedList.size()-1);
    }
    return returnedList;
  }

}
