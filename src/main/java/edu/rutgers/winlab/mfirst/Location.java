/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.rutgers.winlab.mfirst;

import java.util.ArrayList;

/**
 *
 * @author Yi Hu <yihu@winlab.rutgers.edu>
 */
public class Location {
    public String city;
    public String state;
    public String country;
    public ArrayList<String> insertedGUIDs;
    
    public Location (String cityName, String stateName, String countryName){
        city = cityName;
        state = stateName;
        country = countryName;
        insertedGUIDs = new ArrayList<String>();
    }
    
    public String getFullName(){
        String fullName = city +","+state+","+country;
        return fullName;
    }
    
}
