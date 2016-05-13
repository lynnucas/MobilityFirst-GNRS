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
package edu.rutgers.winlab.mfirst.mapping.sid;

/**
 * Configuration object for SID GUID mapper.
 * 
 * @author Yi Hu
 * 
 */
public class Configuration {
  /**
   * The hashing algorithm to use when converting a GUID to a Network Address
   * for insert or retrieval in the GNRS.
   */
  private String hashAlgorithm = "MD5";

  /**
   * Filename for SID to subAS no mapping.
   */
  private String sidsubASFile = "src/main/resources/sid.txt";

  /**
   * Filename for SID to subAS no mapping.
   */
  private String sidGeoFile = "src/main/resources/asInfo.txt";
  
  /**
   * Network routing matrix file. Should be changed together with
   * mapping configuration file.
   */
  private String routingConfiguration = "2220IXP_Prov4_IXP2_m_route.txtinclude_intra";
  
  /**
   * Output file name for the generated insertion workload trace. Should match with topology
   * (i.e., routingConfiguration)
   */
  private String insertionOutFile = "2220IXP_Prov4_IXP2_m_.Insertion";
  
  /**
   * Total number of GUIDs to be inserted 
   */
  private int totalInsGUID = 10000;
  
  /**
   * Total number of queries on each GUID inserted 
   */
  private int queryNoPerGUID = 5;
  
  /**
   * Percentage of queries come from the same country as the GUID 
   */
  private double regionalLocality = 0.8;

  
  /**
   * Filename for AS network binding values.
   */
  private String asBindingFile = "src/main/resources/as-binding.ipv4";

  
  /**
   * Gets the insertionOutFile name.
   * 
   * @return the insertionOutFile name.
   */
  public String getInsertionOutFile() {
    return this.insertionOutFile;
  }

  /**
   * Sets the insertionOutFile name.
   * 
   * @param insOutFile 
   *          the new insertionOutFile name.
   */
  public void setInsertionOutFile(final String insOutFile) {
    this.insertionOutFile = insOutFile;
  }
  
  
  public int getQueryPerGUID(){
      return this.queryNoPerGUID;
  }
  
  public void setQueryPerGUID(final int queries){
      this.queryNoPerGUID = queries>1? queries:1;
  }
  
  
  public double getRegionalLocality(){
      return this.regionalLocality;
  }
  
  public void setRegionalLoclaity(final double regional){
      this.regionalLocality = (regional>=0 && regional <=1)? regional:0.8;
  }
  /**
   * Gets the number of totalInsGUID.
   * 
   * @return the string form of totalInsGUID.
   */
  public int getTotalInsGUID() {
    return this.totalInsGUID;
  }

  /**
   * Sets the totalInsGUID.
   * 
   * @param totalGUIDs 
   *          the new totalInsGUID.
   */
  public void setTotalInsGUID(final int totalGUIDs) {
    this.totalInsGUID = totalGUIDs>1?totalGUIDs:1;
  }
  /**
   * Gets the hash algorithm name.
   * 
   * @return the hash algorithm name.
   */
  public String getHashAlgorithm() {
    return this.hashAlgorithm;
  }

  /**
   * Sets the hash algorithm name.
   * 
   * @param hashAlgorithm
   *          the new algorithm name.
   */
  public void setHashAlgorithm(final String hashAlgorithm) {
    this.hashAlgorithm = hashAlgorithm;
  }

  /**
   * Gets the name of the file that contains the SID to subAS no. mapping.
   * 
   * @return the name of the SID file.
   */
  public String getSIDsubASFile() {
    return this.sidsubASFile;
  }

  /**
   * Sets the name of the file that contains the SID to subAS no. mapping.
   * 
   * @param sidFile
   *          the new SID filename.
   */
  public void setSIDsubASFile(final String sidFile) {
    this.sidsubASFile = sidFile;
  }
  
  /**
   * Gets the name of the file that contains the SID to city name (geo-location) mapping.
   * 
   * @return the name of the asInfo file.
   */
  public String getSIDGeoFile() {
    return this.sidGeoFile;
  }

  /**
   * Sets the name of the file that contains the SID to city name (geo-location) mapping.
   * 
   * @param asFile
   *          the new asInfo filename.
   */
  public void setSIDGeoFile(final String asFile) {
    this.sidGeoFile = asFile;
  }
  
  /**
   * Get the name of the routing matrix file.
   * 
   * @return the routing matrix filename.
   */
  public String getRoutingConfiguration() {
    return this.routingConfiguration;
  }

  /**
   * Sets the routing matrix filename.
   * 
   * @param routeConfig 
   *          the new routing matrix filename.
   */
  public void setRoutingConfiguration(final String routeConfig) {
    this.routingConfiguration = routeConfig;
  }

  /**
   * Gets the name of the Autonomous System (AS) bindings file. The file
   * contains the AS number and network address value for the GNRS server in
   * that system.
   * 
   * @return the name of the AS bindings file.
   */
  public String getAsBindingFile() {
    return this.asBindingFile;
  }

  /**
   * Sets the name of the Autonomous System (AS) bindings file. The file
   * contains the AS number and network address value for the GNRS server in
   * that system.
   * 
   * @param asBindingFile
   *          the name of the AS bindings file.
   */
  public void setAsBindingFile(final String asBindingFile) {
    this.asBindingFile = asBindingFile;
  }
}
