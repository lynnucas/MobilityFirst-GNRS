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
import java.util.Collection;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.rutgers.winlab.mfirst.net.AddressType;
import edu.rutgers.winlab.mfirst.messages.MessageType;
import edu.rutgers.winlab.mfirst.net.NetworkAddress;
import edu.rutgers.winlab.mfirst.mapping.ipv4udp.MessageDigestHasher;
/**
 * A globally-unique identifier represented by a 160-bit (20-byte) data value.
 * <p>
 * A Globally-Unique IDentifier (GUID) is one of the primary data structures in
 * GNRS. It servers as a unique identifier for hosts, content, contexts, and
 * other concepts in the Mobility First (MF) Future Internet Architecture (FIA)
 * project. In the context of GNRS, it may not represent an actual GUID, but
 * instead a cryptographic hash value of a GUID. The reason for this distinction
 * is that a "proper" GUID value may be significantly longer and include
 * public/private components.
 * </p>
 * 
 * @author Robert Moore
 */
public class GUID {

  /**
   * Number of bytes in a GUID value.
   */
  public static final int SIZE_OF_GUID = 20;
  //public static final int SIZE_OF_BYTES = 20;
  public static MessageDigestHasher hasher = new MessageDigestHasher("SHA-1");
  private static final Logger LOG = LoggerFactory
      .getLogger(GUID.class);
  /**
   * Binary representation of the GUID.
   */
  protected transient byte[] bytes;
  protected byte[] hashedBytes;
  
  protected int hashedIntValue;
  public boolean setHashedInt;
  
  protected String city;
  protected String state;
  protected String country;
  private MessageType requestType = MessageType.UNKNOWN;
  /**
   * Converts the specified ASCII-encoded String to a GUID. More specifically,
   * the raw bytes of asString, when ASCII-encoded, are stored in the GUID
   * field. The resulting GUID will be truncated or padded with zeros as
   * necessary.
   * 
   * @param stringValue
   *          the String to convert.
   * @return a GUID with the value of the String
   * @throws UnsupportedEncodingException
   *           if the String cannot be decoded to ASCII characters
   */
  public static GUID fromASCII(final String stringValue) throws UnsupportedEncodingException {
    final GUID guid = new GUID();
    guid.setHashedInt = false;
    if (stringValue != null && stringValue.length() > 0) {
      guid.setBinaryForm(Arrays.copyOf(stringValue.getBytes("US-ASCII"), SIZE_OF_GUID));
      Collection<NetworkAddress>  returnedAddress = hasher.hash(guid, AddressType.GUID, 1);
        for (NetworkAddress n : returnedAddress) {
            //guid.hashedBytes = n.getValue();
            guid.setHashedBinaryForm(Arrays.copyOf(n.getValue(),SIZE_OF_GUID));
            guid.hashedIntValue = guid.getHashedIntForm();
            guid.setHashedInt = true;
        }
    }
    return guid;
  }

  /**
   * Creates a new GUID from an integer. The 4 bytes of the integer are placed
   * in the first 4 bytes of the GUID value. The remaining bytes are padded with
   * 0's.
   * 
   * @param intValue
   *          the integer value.
   * @return a GUID with the integer in its high (first) 4 bytes.
   */
  public static GUID fromInt(final int intValue) {
    final GUID guid = new GUID();
    guid.setHashedInt = false;
    guid.bytes = new byte[SIZE_OF_GUID];
    guid.bytes[0] = (byte) (intValue >> 24);
    guid.bytes[1] = (byte) (intValue >> 16);
    guid.bytes[2] = (byte) (intValue >> 8);
    guid.bytes[3] = (byte) (intValue);
    Collection<NetworkAddress>  returnedAddress = hasher.hash(guid, AddressType.GUID, 1);
    for (NetworkAddress n : returnedAddress) {
        guid.hashedBytes = n.getValue();
    }
    return guid;
  }
  
  
  public static GUID copyGUID(final GUID aguid){
      final GUID guid = new GUID();
      guid.bytes = aguid.getBinaryForm();
      guid.city = aguid.getCity();
      guid.state = aguid.getState();
      guid.country = aguid.getCountry();
      return guid;
  }
          
          
    public byte[] getHashedBinaryForm() {
    return this.hashedBytes;
  }
    
    public int getHashedIntForm() {
        if (setHashedInt) {
            return this.hashedIntValue;
        }
        if (this.hashedBytes==null || this.hashedBytes.length==0) {
            if (this.bytes == null) {
                LOG.info("in getHashedIntForm error hashedvalue={},guid={}",this.hashedBytes,this.toString());
                return -1;
            } else {
                Collection<NetworkAddress>  returnedAddress = hasher.hash(this, AddressType.GUID, 1);
                for (NetworkAddress n : returnedAddress) {
                //this.hashedBytes = n.getValue();
                this.setHashedBinaryForm(Arrays.copyOf(n.getValue(),SIZE_OF_GUID));
                this.hashedIntValue = this.getHashedIntForm();
                this.setHashedInt = true;
                }
            }
            
        } /*return this.hashedBytes[0] << 24 | 
                (this.hashedBytes[1] & 0xFF) << 16 | 
                (this.hashedBytes[2] & 0xFF) << 8 | 
                (this.hashedBytes[3] & 0xFF);*/
            
            ByteBuffer bb = ByteBuffer.wrap(this.hashedBytes);
            int returnedValue = bb.getInt();
            this.setHashedInt = true;
            this.hashedIntValue = returnedValue;
            return returnedValue;
    
    /*int result = 0;
    for (int i=0; i<4; i++) {
      result = ( result << 8 ) - Byte.MIN_VALUE + (int) this.hashedBytes[i];
    }
    return result;*/
  }
  
    public void setHashedBinaryForm(byte[] hashedVal) {
    this.hashedBytes = hashedVal;
  }
  /**
   * Gets this GUID as a byte array.
   * 
   * @return this GUID in binary form.
   */
  public byte[] getBinaryForm() {
    return this.bytes;
  }

  public void setRequestType (final MessageType type){
      this.requestType = type;
  }
  
  public MessageType getRequestType(){
      return this.requestType;
  }
  
  public byte getReqestTypeValue(){
      return this.requestType.value();
  }
  
  public void setCity(final String cityName) {
    this.city = cityName;
  }
  public void setState(final String stateName) {
    this.state = stateName;
  }
  public void setCountry(final String countryName) {
    this.country = countryName;
  }
  public String getCity(){
      return this.city;
  }
  
  public String getState(){
      return this.state;
  }
  
  public String getCountry(){
      return this.country;
  }
  /**
   * Sets the binary form of this GUID from a byte array.
   * 
   * @param guid
   *          the new value of this GUID.
   */
  public void setBinaryForm(final byte[] guid) {
    this.bytes = guid;
  }

  @Override
  public String toString() {
    final StringBuilder sBuff = new StringBuilder(SIZE_OF_GUID * 2 + 6);
    sBuff.append("GUID(");
    for (final byte b : this.bytes) {
      sBuff.append(String.format("%02x", Byte.valueOf(b)));
    }
    sBuff.append(')');
    return sBuff.toString();
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.bytes);
  }

  @Override
  public boolean equals(final Object otherObject) {
    return (otherObject instanceof GUID) ? this.equalsGUID((GUID) otherObject)
        : super.equals(otherObject);

  }

  /*
  length of city, state and country, plus three length numbers
  */
  public int getLocationLengh(){
      int length =0;
      if (this.getCity()!=null && this.getState()!=null && this.getCountry()!= null) {
          length += this.getCity().getBytes().length + this.getState().getBytes().length+
                  this.getCountry().getBytes().length+ 6; //3 unsigned short to indicate the size for city, state, country
      }
      return length;
      
  }
  /**
   * Determines if this GUID and another are equal based on their binary
   * representations.
   * 
   * @param guid
   *          another GUID.
   * @return {@code true} if the other GUID'd binary value is equal to this
   *         GUID's.
   */
  public boolean equalsGUID(final GUID guid) {
    return Arrays.equals(this.bytes, guid.bytes);
  }
}
