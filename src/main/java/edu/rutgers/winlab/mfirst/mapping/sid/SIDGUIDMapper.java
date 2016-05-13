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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Writer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.InetAddress;
//import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.lang.Math;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thoughtworks.xstream.XStream;

import edu.rutgers.winlab.mfirst.GUID;
import edu.rutgers.winlab.mfirst.SID;
import edu.rutgers.winlab.mfirst.Location;
import edu.rutgers.winlab.mfirst.mapping.GUIDMapper;
import edu.rutgers.winlab.mfirst.mapping.ipv4udp.GUIDHasher;
import edu.rutgers.winlab.mfirst.mapping.ipv4udp.MessageDigestHasher;
import edu.rutgers.winlab.mfirst.net.AddressType;
import edu.rutgers.winlab.mfirst.net.NetworkAddress;
import edu.rutgers.winlab.mfirst.net.ipv4udp.IPv4UDPAddress;


/**
 * GUID Mapper for GUID to SID.
 * 
 * @author Yi Hu
 */
public class SIDGUIDMapper implements GUIDMapper {

  /**
   * Logging for this class.
   */
  private static final Logger LOG = LoggerFactory
      .getLogger(SIDGUIDMapper.class);
  /**
   * Sequence number of insertion trace.
   */
  private static AtomicInteger Ins_SeqNo = new AtomicInteger(0);
  
  private final transient Random rnd; 
  
  /**
   * Local InetSocketAddr
   */
  private final InetSocketAddress localSocketAddr;
  
  private Integer localSubASno = null;

  /**
   * Hashing object to compute a random network address.
   */
  private final transient GUIDHasher hasher;
  /**
   * Mapping of SIDs to subAS number. A subAS is a part of AS within a single country, as a unit in topology distance calculation.
   */
  public final transient ConcurrentHashMap<Integer, Integer> sidSubASMap = new ConcurrentHashMap<Integer, Integer>();

  /**
   * Mapping city names to the number of PoPs deployed in the city. Used for generating geo-aware insertion and lookup workload.
   */
  public final transient ConcurrentHashMap<String, Integer> popCntPerCity = new ConcurrentHashMap<String, Integer>();

  /**
   * Mapping of sub AS numbers to their network locations.
   */
  public final transient ConcurrentHashMap<Integer, InetSocketAddress> subASAddresses = new ConcurrentHashMap<Integer, InetSocketAddress>();

  /**
   * List of all SIDs.
   */
   public ArrayList<SID> sidList = new ArrayList<SID>();
   
   /**
   * List of all locations where there is a GNRS deployed.
   */
   public ArrayList<Location> locationList = new ArrayList<Location>();
   
   /**
   * The number of subAS. The ID of subAS is from 0 to (numSubAS-1)
   */
  public int numSubAS;
   /**
   * 
   * routing distance matrix from subAS to subAS
   */
   public ArrayList<Integer> routeDist = new ArrayList<Integer>();
   
   /**
   * 
   * Statistic recording GUID mapping distribution among SIDS.
   */
   public ArrayList<Integer> mappCountList = new ArrayList<Integer>();
  /**
   * Creates a new IPv4+UDP GUID mapper from the specified configuration
   * filename. The configuration file is opened, parsed, and this mapper is
   * configured.
   * 
   * @param configFile
   *          the name of the configuration file for this mapper.
   * @throws IOException
   *           if an IOException is thrown while reading the configuration file.
   */
  public SIDGUIDMapper(final String configFile, InetSocketAddress localSocketAddress) throws IOException {
      
    this.rnd = new Random(System.currentTimeMillis());
    this.localSocketAddr=localSocketAddress;
    LOG.info("localSocketAddress passed to SIDGUIDMapper is {}.",this.localSocketAddr);
    final Configuration config = this.loadConfiguration(configFile);
    this.hasher = new MessageDigestHasher(config.getHashAlgorithm());
    //Load SID subAS mapping info
    this.loadSIDASInfo(config.getSIDsubASFile());
    //Load SID geo info
    this.loadSIDGeoInfo(config.getSIDGeoFile());
    //test popCityCnt
    LOG.info("popCntPerCity size is {}.",popCntPerCity.size());
    //load routing delay info
    this.loadRoutingDistance(config.getRoutingConfiguration());
    // Load the AS network address binding values
    this.loadAsNetworkBindings(config.getAsBindingFile());
    
    for (int i = 0; i < sidList.size(); i++) {
          mappCountList.add(0);
    }
    
    
    getLocalsubASNo();
    //the following two functions are only for simulating test
    //multiServerArtificalBinding();
    //singleServerArtificalBinding();
    
    String outInsertion = config.getInsertionOutFile()+"_"+config.getTotalInsGUID();
    generateInsertionTrace(config.getTotalInsGUID(), config.getQueryPerGUID(),
            config.getRegionalLocality(),outInsertion);
    //testGUIDMapping ("/Users/yihu/Documents/gnrs_implementation/mf/gnrs/jserver/mapPdf.csv");
  }
  
  private void multiServerArtificalBinding(){
    LOG.info("Original subASADdresses.size ={}",subASAddresses.size());
    InetSocketAddress asGNRSAddr =null, currasGNRSAddr=null;
    asGNRSAddr = subASAddresses.get(subASAddresses.size()-1);
    InetAddress asIPAddr = asGNRSAddr.getAddress();
    int asPort = asGNRSAddr.getPort();
    int currAS = subASAddresses.size();
    while (currAS < this.numSubAS) {
        currasGNRSAddr = new InetSocketAddress(asIPAddr,++asPort);
        subASAddresses.put(currAS,currasGNRSAddr);
        currAS++;
    }
    LOG.info("Inserted multiServer subASADdresses.size ={}",subASAddresses.size());
  }
  
  private void singleServerArtificalBinding(){
    LOG.info("Original subASADdresses.size ={}",subASAddresses.size());
    assert(this.localSocketAddr!=null);
    InetSocketAddress asGNRSAddr =null, currasGNRSAddr=null;
    asGNRSAddr = this.localSocketAddr;
    InetAddress asIPAddr = asGNRSAddr.getAddress();
    int asPort = asGNRSAddr.getPort();
    int currAS = 0;
    while (currAS < this.numSubAS) {
        currasGNRSAddr = new InetSocketAddress(asIPAddr,asPort);
        subASAddresses.put(currAS,currasGNRSAddr);
        currAS++;
    }
    LOG.info("Inserted singleServer subASADdresses.size ={}",subASAddresses.size());
      
  }
  
  /*retrieve the local subAS no through local socketAddress
  */
  private void getLocalsubASNo(){
      assert(this.localSocketAddr!=null && subASAddresses.size()>0);
      Iterator it = subASAddresses.keySet().iterator();
        while(it.hasNext()) {
        Integer key= (Integer)it.next(); 
        InetSocketAddress value= subASAddresses.get(key);
        /*if(! (value== null)){ //check for null
            LOG.info("subASAddresses has key {} value {}.",key, value);
        }*/
        if (value!=null && value.equals(this.localSocketAddr)) {
            localSubASno = key;
            LOG.info("loca subAS no. = {}.",localSubASno);
            break;
        }
    }
  }
  
  /**
   * Loads this Mapper's configuration file from the filename provided.
   * 
   * @param filename
   *          the name of the configuration file.
   * @return the configuration object
   */
  private Configuration loadConfiguration(final String filename) {
    final XStream xStream = new XStream();
    return (Configuration) xStream.fromXML(new File(filename));
  }
  /**
   * Loads the routing latency from subAS to subAS including intra subAS latency.
   * 
   * @param routeFileName
   *          the name of the routing delay file.
   * @throws IOException
   *           if an IOException is thrown while reading the routing file.
   */
  private void loadRoutingDistance(String routeFileName) throws IOException {
    final File routeFile = new File(routeFileName);
    final BufferedReader lineReader = new BufferedReader(new FileReader(
        routeFile));
    String line = lineReader.readLine();
    this.numSubAS = Integer.parseInt(line);
    routeDist = new ArrayList<Integer>(this.numSubAS*this.numSubAS);
    LOG.info("subAS no {} in loadRoutingDistance.",this.numSubAS);
    line = lineReader.readLine();
    while (line != null) {
      // Eliminate leading/trailing whitespace
      line = line.trim();
      // Skip comments
      if (line.length() == 0 || line.charAt(0) == '#') {
        line = lineReader.readLine();
        continue;
      }
      final String[] generalComponents = line.split("\\s+");
        if (generalComponents.length ==this.numSubAS) {
            for (int i = 0; i < generalComponents.length; i++) {
                if (Long.parseLong(generalComponents[i]) > Integer.MAX_VALUE) {
                    this.routeDist.add(50000);
                    LOG.error("Outrange distance = {}.",generalComponents[i]);
                } else {
                    this.routeDist.add(Integer.parseInt(generalComponents[i]));
                }
            }
        }
        else{
            LOG.error("Fail to add in loadRoutingDistance, length = {}.",generalComponents.length);
        }
     line = lineReader.readLine();
    }
    lineReader.close();
    LOG.info("routeDist size is {}.",this.routeDist.size());
      
  }
  
  /**
   * Loads the Autonomous System (AS) network bindings file.
   * 
   * @param sidFilename
   *          the name of the sid geo information file.
   * @throws IOException
   *           if an IOException is thrown while reading the bindings file.
   */
  private void loadSIDGeoInfo(final String sidFilename)
      throws IOException {
    final File sidASFile = new File(sidFilename);
    final BufferedReader lineReader = new BufferedReader(new FileReader(
        sidASFile));
    int thisSIDno=0;
    String cityName, stateName, countryName;
    SID currSIDObj;
    String line = lineReader.readLine();
    while (line != null) {
      // Eliminate leading/trailing whitespace
      line = line.trim();
      // Skip comments
      if (line.length() == 0 || line.charAt(0) == '#') {
        line = lineReader.readLine();
        continue;
      }
      final String[] generalComponents = line.split("\\$");
      if (generalComponents.length == 2) {
          final String[] generalComponents3 = generalComponents[0].split(",");
          if (generalComponents3.length == 2) { // only processing intra-PoPs per AS, ignore intra-PoP connections per AS
           final String[] generalComponents2 = generalComponents[1].split(";");
            for (int i = 0; i < generalComponents2.length; i++) {
              final String[] generalComponents4 = generalComponents2[i].split(",");
                if (generalComponents4.length==3) {
                    cityName=generalComponents4[0];
                    stateName=generalComponents4[1];
                    countryName=generalComponents4[2];
                    currSIDObj=new SID(thisSIDno,cityName,stateName,countryName);
                    this.sidList.add(currSIDObj);
                    thisSIDno++;
                    // collect the city name to record pop counts per city
                    if (popCntPerCity.containsKey(generalComponents2[i])) {
                        popCntPerCity.put(generalComponents2[i], popCntPerCity.get(generalComponents2[i])+1);
                    } else { 
                        popCntPerCity.put(generalComponents2[i],1);
                    }
                }
            }   
         }
      }
      line = lineReader.readLine();
    }
    lineReader.close();
    LOG.info("sidList.size {}",sidList.size());
    LOG.info("Finished loading SID Geo file.");
  }
  
  

  /**
   * Loads the Autonomous System (AS) network bindings file.
   * 
   * @param sidFilename
   *          the name of the sid subAS information file.
   * @throws IOException
   *           if an IOException is thrown while reading the bindings file.
   */
  private void loadSIDASInfo(final String sidFilename)
      throws IOException {
    final File sidASFile = new File(sidFilename);
    final BufferedReader lineReader = new BufferedReader(new FileReader(
        sidASFile));
    int totalAS, totalsubAS, currTotalSubAS=0, currTotalSID=0;
    int thisSubASno=0, thisSIDno=0;
    Integer thisSubAS, thisSID;
    String line = lineReader.readLine();
    while (line != null) {
      // Eliminate leading/trailing whitespace
      line = line.trim();
      // Skip comments
      if (line.length() == 0 || line.charAt(0) == '#') {
        line = lineReader.readLine();
        continue;
      }

      // LOG.debug("Parsing \"{}\"", line);
      // Extract any comments and discard
      final String content = line.split("#")[0];

      // Extract the 3 parts (AS #, IP address, port)
      final String[] generalComponents = content.split("\\s+");
      if (generalComponents.length == 2) {
        totalAS = Integer.parseInt(generalComponents[0]);
        totalsubAS = Integer.parseInt(generalComponents[1]);
        LOG.info("total  no. of AS {}. total no. of subAS {}.", totalAS, totalsubAS);
      }
      else {
          currTotalSubAS = Integer.parseInt(generalComponents[1]);
          if (generalComponents.length != 2*currTotalSubAS+2) {
              LOG.warn("Miss match component number \"{}\".", content);
          }
          for (int i = 1; i <= currTotalSubAS; i++) {
              thisSubAS = new Integer (thisSubASno);
              thisSubASno ++;
              currTotalSID = Integer.parseInt(generalComponents[i*2]);
              for (int j = 0; j < currTotalSID; j++) {
                  thisSID = new Integer (thisSIDno);
                  sidSubASMap.put(thisSID, thisSubAS);
                  thisSIDno ++;
                  //LOG.info("sidSubASMap put {} {}", thisSID.intValue(), thisSubAS.intValue());
              }             
          }
      }
      line = lineReader.readLine();
    }
    lineReader.close();
    LOG.info("Finished loading SID subAS file.");
    LOG.info("sidSubASMap.size {}",sidSubASMap.size());
  }
  
  
  /**
   * Loads the Autonomous System (AS) network bindings file.
   * 
   * @param asBindingFilename
   *          the name of the AS bindings file.
   * @throws IOException
   *           if an IOException is thrown while reading the bindings file.
   */
  private void loadAsNetworkBindings(final String asBindingFilename)
      throws IOException {
    final File asBindingFile = new File(asBindingFilename);
    final BufferedReader lineReader = new BufferedReader(new FileReader(
        asBindingFile));

    String line = lineReader.readLine();
    while (line != null) {
      // Eliminate leading/trailing whitespace
      line = line.trim();
      // Skip comments
      if (line.length() == 0 || line.charAt(0) == '#') {
        line = lineReader.readLine();
        continue;
      }

      // log.debug("Parsing \"{}\"", line);
      // Extract any comments and discard
      final String content = line.split("#")[0];

      // Extract the 3 parts (AS #, IP address, port)
      final String[] generalComponents = content.split("\\s+");
      if (generalComponents.length < 3) {
        LOG.warn("Not enough components to parse the line \"{}\".", line);
        line = lineReader.readLine();
        continue;
      }

      final Integer asNumber = Integer.valueOf(generalComponents[0]);
      final String ipAddrString = generalComponents[1];
      final int port = Integer.parseInt(generalComponents[2]);

      final InetSocketAddress sockAddx = new InetSocketAddress(ipAddrString,
          port);
      this.subASAddresses.put(asNumber, sockAddx);

      line = lineReader.readLine();
    }
    lineReader.close();
    LOG.info("Finished loading AS network binding values.");

  }

  /*
   * get global replicas as well as one regional and one local replica for a guid
   * the returned addresses are ordered by the routing distance from local server to the replica server
   * @param numAddresses: the number of global replicas
   */
  @Override
  public Collection<NetworkAddress> getMapping(final GUID guid,
      final int numAddresses, final AddressType... types) {
      // if getMapping is called, force mapping from a GUID to SIDs
    List<AddressType> returnedTypes;
    returnedTypes = new ArrayList<AddressType>();
    returnedTypes.add(this.getDefaultAddressType());

    final ArrayList<NetworkAddress> returnedAddresses = new ArrayList<NetworkAddress>();

    for (final AddressType type : returnedTypes) {
      returnedAddresses
          .addAll(this.getAddressForType(type, guid, numAddresses));

    }
    if (returnedAddresses.isEmpty()) {
      return null;
    }
    return returnedAddresses;
  }

  /**
   * Returns a collection of mapped NetworkAddresses, randomly generated from
   * {@code guid} of the specified type.
   * 
   * @param type
   *          the type of address to create.
   * @param guid
   *          the GUID to use to generate the addresses.
   * @param numAddresses
   *          the number of global replica's addresses to create.
   * @return a collection containing the addresses of the specified number of global 
   * replicas and 1 regional replica and 1 local replica, if they were able to be
   *         generated.
   */
  private Collection<NetworkAddress> getAddressForType(final AddressType type,
      final GUID guid, final int numAddresses) {
      //assert((numAddresses+2) <= this.sidList.size());
      final LinkedList<NetworkAddress> returnedAddresses = new LinkedList<NetworkAddress>();
    
      ArrayList<SID> candidateSIDS = this.sidList;
      ArrayList<SID> selectedSIDs = new ArrayList<SID>();
      int currDistance;
      for (int i = 0; i < candidateSIDS.size(); i++) {
           currDistance = Math.abs(guid.getHashedIntForm()- this.sidList.get(i).getHashedIntForm());
           candidateSIDS.get(i).setCurrDistance(currDistance);
      }
      Collections.sort(candidateSIDS, new SIDDistanceComparator());
        for (int i = 0; i < numAddresses; i++) {
            if (i<candidateSIDS.size()) {
                selectedSIDs.add(candidateSIDS.get(i));
            }
        }
        int indexToContinue =-1;
        for (int i = numAddresses; i < candidateSIDS.size(); i++) {
            if (guid.getCountry().compareTo(candidateSIDS.get(i).getCountry())==0) {
                selectedSIDs.add(candidateSIDS.get(i));
                indexToContinue = i+1;
                break;
            }
        }
        if (indexToContinue>0) {
            for (int i = indexToContinue; i < candidateSIDS.size(); i++) {
                if (guid.getCountry().compareTo(candidateSIDS.get(i).getCountry())==0 && 
                    guid.getState().compareTo(candidateSIDS.get(i).getState()) ==0
                    && guid.getCity().compareTo(candidateSIDS.get(i).getCity()) ==0 ) {
                selectedSIDs.add(candidateSIDS.get(i));
                break;
                }
            }   
        } 
        
        //LOG.info("Target GUID at is at {},{}",guid.getCountry(),guid.getCity());
        for (SID sid:selectedSIDs) {
          Integer n = mappCountList.get(sid.getSIDIntValue());
          n = Integer.valueOf(n.intValue() + 1);
          mappCountList.set(sid.getSIDIntValue(),n);
          //LOG.info("selectedSIDs country is{} city is {}",sid.getCountry(),sid.getCity());
      }
        Integer currSubASno;
        for (int i = 0; i < selectedSIDs.size(); i++) {
          currSubASno = this.sidSubASMap.get(selectedSIDs.get(i).getSIDIntValue());
            if (this.localSubASno!= null && currSubASno != null &&
                    currSubASno.intValue() >=0 && currSubASno.intValue() < this.numSubAS) {
                currDistance = this.routeDist.get(this.localSubASno*this.numSubAS+currSubASno.intValue());
            } else{
                LOG.error("Unable to find routing distance from subAS{} to {}", this.localSubASno, currSubASno);
                currDistance = Integer.MAX_VALUE;
            }
            selectedSIDs.get(i).setCurrDistance(currDistance);
      }
        Collections.sort(selectedSIDs, new SIDDistanceComparator());
        //LOG.info("Following SIDs should be ordered");
      // Map them to an AS
      for (final SID currSid : selectedSIDs) {
        final NetworkAddress finalAddr = this.performMapping(currSid);
        //LOG.info("selected SID {} routing distance is {} ",currSid.getSIDIntValue(),
                //currSid.getCurrDistance());
        //LOG.info("address is {}", finalAddr);
        if (finalAddr == null) {
          LOG.error("Unable to map NetworkAddress for {}", currSid);
        } else {
          returnedAddresses.add(finalAddr);
        }
      }
    
    return returnedAddresses;
  }
    class SIDDistanceComparator implements Comparator<SID> {
    public int compare(SID sid1, SID sid2) {
        return (sid1.getCurrDistance()-sid2.getCurrDistance());
    }
}
  /**
   * Maps a network address to an AS based on the announced prefix table.
   * 
   * @param netAddr
   *          a random network address.
   * @return the NetworkAddress of the GNRS server of the AS "responsible" for
   *         {@code netAddr}
   */
  private NetworkAddress performMapping(final SID hostSID) {
    Integer currSID = new Integer(hostSID.getSIDIntValue());
    Integer autonomousSystem = this.sidSubASMap.get(currSID);
    NetworkAddress finalAddr;
    if (autonomousSystem == null) {
      LOG.error("Cannot find subAS for SID {}", hostSID.getSIDIntValue());
      finalAddr = null;
    } else {
      final InetSocketAddress asGNRSAddr = this.subASAddresses.get(autonomousSystem);
        if (asGNRSAddr == null) {
            LOG.error("Cannot find InetSocketAddr for subAS {}", autonomousSystem);
            finalAddr=null;
        } else{
            finalAddr = IPv4UDPAddress.fromInetSocketAddress(asGNRSAddr);
        }
      
    }
    LOG.info("zzz sid map to network {}", finalAddr);
    return finalAddr;
  }

  @Override
  public EnumSet<AddressType> getTypes() {
    return EnumSet.of(AddressType.SID);
  }

  @Override
  public AddressType getDefaultAddressType() {
    return AddressType.SID;
  }
  
  public void testGUIDMapping(String filePath) throws IOException {
      LOG.info("begin in testGUIDMapping");
          
        for (int i = 0; i < sidList.size(); i++) {
          GUID guid = GUID.fromInt(i);
          guid.setCountry(sidList.get(i).getCountry());
          guid.setState(sidList.get(i).getState());
          guid.setCity(sidList.get(i).getCity());
          Collection<NetworkAddress> mapped = getMapping(guid, 5,AddressType.SID);
          LOG.info("in testGUID i={}",i);
      }
      ArrayList<Double> currMapCounts = new ArrayList<Double>();
      for (int i = 0; i < mappCountList.size(); i++) {
          LOG.info("mappCountList i={}{}",i,mappCountList.get(i));
          currMapCounts.add(mappCountList.get(i)/7.00);
      }
      Collections.sort(currMapCounts);
      File file = new File(filePath);
       Writer fileWriter = null;
       BufferedWriter bufferedWriter = null;
       try {
        if (!file.exists()) {
            file.createNewFile();
        }
        fileWriter = new FileWriter(file);
        bufferedWriter = new BufferedWriter(fileWriter);
        double pcent = 0.0;
        double idx = 0.0;
        for (int i=0; i<currMapCounts.size(); i++) {
        idx ++;
        if ((i+1)< currMapCounts.size()) {
            if (currMapCounts.get(i)<currMapCounts.get(i+1)) {
                pcent = idx/currMapCounts.size();
                bufferedWriter.write(Double.toString(pcent));
                bufferedWriter.write('\t');
                bufferedWriter.write(Double.toString(currMapCounts.get(i)));
                bufferedWriter.newLine();
                LOG.info("pcent {} {}", pcent,currMapCounts.get(i) );
                idx = 0.0;
            }
        }
        else{
            pcent = idx/currMapCounts.size();
            bufferedWriter.write(Double.toString(pcent));
            bufferedWriter.write('\t');
            bufferedWriter.write(Double.toString(currMapCounts.get(i)));
            LOG.info("pcent {} {}", pcent,currMapCounts.get(i) );
        }
        }
        bufferedWriter.close();
        fileWriter.close();
        } catch (IOException e) {
            System.err.println("Error writing the file : ");
            e.printStackTrace();
        } 
     LOG.info("finish in testGUIDMapping");
      
  }
  /* 
  * Format a single line of Insertion Trace
  */
  private String writeSingleInsertion(String location){
      //String results = Integer.toString(Ins_SeqNo.incrementAndGet())+ 
              //" I "+Integer.toString(Ins_SeqNo.get())+ " "+
              //Integer.toString(Ins_SeqNo.get()) + ",9999,1"+ " "+location+"\n";
      StringBuffer buffer = new StringBuffer();
      buffer.append(Integer.toString(Ins_SeqNo.incrementAndGet()));
      buffer.append(" I ");
      buffer.append(Integer.toString(Ins_SeqNo.get()));
      buffer.append(" ");
      buffer.append(Integer.toString(Ins_SeqNo.get()));
      buffer.append(",9999,1 ");
      buffer.append(location);
      buffer.append("\n");
      return buffer.toString();
  }
  
  class LocationComparator implements Comparator<Location> {
    public int compare(Location loc1, Location loc2) {
        if (loc1.country.compareToIgnoreCase(loc2.country) != 0) {
            return loc1.country.compareToIgnoreCase(loc2.country);
        } else if (loc1.state.compareToIgnoreCase(loc2.state) != 0){
            return loc1.state.compareToIgnoreCase(loc2.state);
        } else{
            return loc1.city.compareToIgnoreCase(loc2.city);
        }
    }
}
  /* 
  * Return the list of index into locationList within the same country as Param currCountry
  */
  private ArrayList<Integer> findLocationsByCountry(String currCountry){
      int index=-1;
      ArrayList<Integer> results = new ArrayList<Integer>();
      for (int i = 0; i < locationList.size(); i++) {
          if (locationList.get(i).country.compareToIgnoreCase(currCountry)==0) {
              index=i;
              results.add(index);
              break;
          }
      }
      while (++index < locationList.size() && 
                  locationList.get(index).country.compareToIgnoreCase(currCountry)==0) {
              results.add(index);
      }
      return results;
  }
  
  private String writeQueries4SingleGUID(String currGUID, int regionalNo, 
          int globalNo, ArrayList<Integer> regionalCandidates ){
      StringBuffer buffer = new StringBuffer();
      int addedQueries=0;
      int idxToLocationList;
      while (addedQueries< regionalNo) {
          buffer.append(Integer.toString(Ins_SeqNo.incrementAndGet()));
          idxToLocationList = regionalCandidates.get(rnd.nextInt(regionalCandidates.size()));
          buffer.append(" Q ");
          buffer.append(currGUID);
          buffer.append(" ");
          buffer.append(locationList.get(idxToLocationList).getFullName());
          buffer.append("\n");
          addedQueries++;
      }
      addedQueries=0;
      while (addedQueries< globalNo) {
          buffer.append(Integer.toString(Ins_SeqNo.incrementAndGet()));
          idxToLocationList = rnd.nextInt(locationList.size());
          buffer.append(" Q ");
          buffer.append(currGUID);
          buffer.append(" ");
          buffer.append(locationList.get(idxToLocationList).getFullName());
          buffer.append("\n");
          addedQueries++;
      }
      return buffer.toString();
  }
  /* 
  * Generate workload trace of Insertion GUIDs
  */
  public void generateInsertionTrace(int totalNoGUIDs, int qPerGUID, double regionalDegree, String outFileName)
  throws IOException {
      
      //Random rnd = new Random(System.currentTimeMillis());
      double totalGUIDs = (double)totalNoGUIDs;
      double totalSIDs = (double)this.sidList.size();
      double currGUIDperCity=0.0, currFloor=0.0, currProb=0.0, currRnd=0.0;
      assert(totalGUIDs >0 && totalSIDs >0);
      
      LOG.info("generateInsertionTrace: totalGUIDs ={}, totalSIDs ={}.", totalGUIDs, totalSIDs);
      Iterator it = popCntPerCity.keySet().iterator();
      
      File file = new File(outFileName);
       Writer fileWriter = null;
       BufferedWriter bufferedWriter = null;
       try {
        if (!file.exists()) {
            file.createNewFile();
        }
        else {
            LOG.info("Insertion trace {} is done! Skip duplicate generation.",outFileName);
            return;
        }
        fileWriter = new FileWriter(file);
        bufferedWriter = new BufferedWriter(fileWriter);
        boolean addNewLocation = false;
        Location currLocation = new Location("0","0","0");
        while(it.hasNext()) {
            String key= (String)it.next(); 
            Integer value= popCntPerCity.get(key);
            if(! (value== null)){ //check for null
                currGUIDperCity = totalGUIDs/totalSIDs*value.intValue();
                currFloor = Math.floor(currGUIDperCity);
                currProb = currGUIDperCity - currFloor;
                currRnd = rnd.nextDouble();
                addNewLocation = false;
                if (currGUIDperCity>=1 || currRnd<= currProb) {
                    String[] locationComponents = key.split(",");
                    assert(locationComponents.length == 3);
                    currLocation = new Location(locationComponents[0],locationComponents[1],locationComponents[2]);
                    addNewLocation = true;
                }
                for (int i = 1; i <= currGUIDperCity; i++) {
                    bufferedWriter.write(writeSingleInsertion(key));
                    currLocation.insertedGUIDs.add(Integer.toString(Ins_SeqNo.get()));
                }
                if (currRnd <= currProb) {
                    bufferedWriter.write(writeSingleInsertion(key));
                    currLocation.insertedGUIDs.add(Integer.toString(Ins_SeqNo.get()));
                }
                if (addNewLocation) {
                    locationList.add(currLocation);
                }
            }
        }
        LOG.info("total insertion generated {}.",Ins_SeqNo.get());
        int regioanlQNo = (int)Math.floor(qPerGUID*regionalDegree);
        int globalQNo = qPerGUID-regioanlQNo;
        Collections.sort(locationList, new LocationComparator());
        assert(locationList.size()>0);
        int index=0;
        String currCountry=locationList.get(index).country;
        ArrayList<Integer> currRegionalCandidate = findLocationsByCountry(currCountry);
        do {
            if (locationList.get(index).country.compareToIgnoreCase(currCountry)!=0) {
                currCountry = locationList.get(index).country;
                currRegionalCandidate = findLocationsByCountry(currCountry); 
            }
            for (int i = 0; i < locationList.get(index).insertedGUIDs.size(); i++) {
                bufferedWriter.write(
                        writeQueries4SingleGUID(locationList.get(index).insertedGUIDs.get(i), 
                                regioanlQNo, globalQNo, currRegionalCandidate));
                
            } 
        } while (++index<locationList.size());
        
        bufferedWriter.close();
        fileWriter.close();
        
        //test locationList
           /*for (int i = 0; i < locationList.size(); i++) {
               LOG.info("locationList {} {} inserted GUIDs:", i, locationList.get(i).getFullName());
               for (int j = 0; j < locationList.get(i).insertedGUIDs.size(); j++) {
                   LOG.info("GUID {}.",locationList.get(i).insertedGUIDs.get(j));
               }
               
           }*/
        } catch (IOException e) {
            System.err.println("Error writing the file : ");
            e.printStackTrace();
        } 
  }
  @Override
  public NetworkAddress getSIDAddrbyLocation(String city, String state, String country){
      NetworkAddress finalAddr = null;
      ArrayList<Integer> candidateSIDs = new ArrayList<Integer>();
      for (int i = 0; i < sidList.size(); i++) {
          if (sidList.get(i).getCity().compareTo(city)==0
                  && sidList.get(i).getState().compareTo(state)==0
                  && sidList.get(i).getCountry().compareTo(country)==0) {
              candidateSIDs.add(i);
            }
      }
      int selectedSIDIndex = candidateSIDs.get(rnd.nextInt(candidateSIDs.size()));
      Integer selectedSID = sidList.get(selectedSIDIndex).getSIDIntValue();
      //LOG.info("selectedSID {}, {}", sidList.get(selectedSIDIndex).getCity(),sidList.get(selectedSIDIndex).getCountry());
      //LOG.info("should match {}, {}", city, country);
      Integer selectedAS = sidSubASMap.get(selectedSID);
      InetSocketAddress selectedASaddr = null;
      if (selectedAS != null) {
          selectedASaddr = subASAddresses.get(selectedAS);
          finalAddr = IPv4UDPAddress.fromInetSocketAddress(selectedASaddr);
          //LOG.info("getSIDAddrbyLocation {}, {}", selectedASaddr, finalAddr);
      }
      
      return finalAddr;
  }
}
