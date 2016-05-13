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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.util.Collection;
import edu.rutgers.winlab.mfirst.net.AddressType;
import edu.rutgers.winlab.mfirst.net.NetworkAddress;
import edu.rutgers.winlab.mfirst.GUID;
import edu.rutgers.winlab.mfirst.mapping.ipv4udp.MessageDigestHasher;
import edu.rutgers.winlab.mfirst.net.AddressType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A globally-unique identifier represented by a 160-bit (20-byte) data value.
 * <p>
 * A Server IDentifier (SID) is the GUID of a GNRS server.
 * </p>
 * 
 * @author Yi Hu
 */
public class SID extends GUID {
  private int intvalue_sid;
  private int currNumDistance; //for sorting SIDs based on their hashed value to a guid's hashed value
  private static final Logger LOG = LoggerFactory
      .getLogger(SID.class);
  
  public SID (final int intValue, String cityName, String stateName, String countryName, byte[] hashvalue){
      GUID guid = super.fromInt(intValue);
      bytes = guid.getBinaryForm();
      intvalue_sid = intValue;
      hashedBytes = hashvalue;
      city = cityName;
      state = stateName;
      country = countryName;
  }
  
   public SID (final int intValue, String cityName, String stateName, String countryName){
      GUID guid = super.fromInt(intValue);
      bytes = guid.getBinaryForm();
      hashedBytes = guid.getHashedBinaryForm();
      intvalue_sid = intValue;
      city = cityName;
      state = stateName;
      country = countryName;
      /*Collection<NetworkAddress>  returnedAddress = hasher.hash(guid, AddressType.GUID, 1);
       for (NetworkAddress n : returnedAddress) {
           hashedBytes = n.getValue();
       }*/
  }
   
   public void setCurrDistance(int distance){
       this.currNumDistance = distance;
   }
   public int getCurrDistance(){
       return this.currNumDistance;
   }
   
   
  @Override
  public String toString() {
    final StringBuilder sBuff = new StringBuilder(SIZE_OF_GUID * 2 + 6);
    sBuff.append("SID(");
    for (final byte b : this.bytes) {
      sBuff.append(String.format("%02x", Byte.valueOf(b)));
    }
    sBuff.append(')');
    return sBuff.toString();
  }

  public int getSIDIntValue(){
      return this.intvalue_sid;
  }
  /**
   * Determines if this GUID and another are equal based on their index.
   * 
   * @param sid
   *          another SID.
   * @return {@code true} if the other GUID'd binary value is equal to this
   *         GUID's.
   */
  public boolean equalsSID(final SID sid) {
    return (this.intvalue_sid == sid.intvalue_sid);
  }
}
