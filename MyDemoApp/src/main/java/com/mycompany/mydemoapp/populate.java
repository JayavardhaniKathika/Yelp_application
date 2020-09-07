/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mydemoapp;

import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;

public class populate {
    public static void main(String[] args) throws ClassNotFoundException{
        populate p=new populate();
        JSONParser jsonParser = new JSONParser();
        String fileName = "/Users/Pranav/Desktop/santa clara/Winter2020/Database_Systems/Assignment/A3/YelpDataset-CptS451/yelp_business.json";
        try 
        { 
            Class.forName("oracle.jdbc.driver.OracleDriver"); 
        } 
        catch(ClassNotFoundException cnfe){ 
               System.out.println("Expeption:"+cnfe);
        }
        try {
            Connection C = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE","System","oracle");
            p.insertbusiness(C);
            p.insertYelpUser(C);
            p.insertcheckin(C);
            p.insertreview(C);
            p.insertcat(C);
            
            p.userDat(C);


            C.close();
        } catch(Exception E){
            System.out.println("SQLException:"+E.getMessage());
        }
        
        
    }
        
    
    private void insertYelpUser(Connection C){
        String fileName = "/Users/Pranav/Desktop/santa clara/Winter2020/Database_Systems/Assignment/A3/YelpDataset-CptS451/yelp_user.json";
        JSONParser jsonParser = new JSONParser();
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException cnfe){
            System.out.println("Expeption:"+cnfe);
        }
        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            
            PreparedStatement statement = C.prepareStatement("Insert into yelp_user(yelping_since,votes,review_count,name,user_id,friends,fans,average_stars,type,compliments,elite) values(to_date(?,'YYYY-MM'),?,?,?,?,?,?,?,?,?,?)");
            
            String  line= reader.readLine();
            int count=1;
            System.out.println("User");
            while (line != null ) {
                if(count%10000==0){
                    System.out.println(count);
                }
                count++;
                Object obj = jsonParser.parse(line);
                JSONObject user = (JSONObject) obj;
                
                String yelping_since=(String)user.get("yelping_since").toString();
                
                statement.setString(1,yelping_since);
               
                
                String votes=(String)user.get("votes").toString();
                statement.setString(2,votes);
               
                Long review_count=(Long)user.get("review_count");
                statement.setLong(3,review_count);
               
                String name=(String)user.get("name").toString();
                statement.setString(4,name);
               
                String user_id=(String)user.get("user_id").toString();
                statement.setString(5,user_id);
              
                String friends=(String)user.get("friends").toString();
                statement.setString(6,friends);
               
                Long fans=(Long)user.get("fans");
                statement.setLong(7, fans);
             
                Double average_stars=(Double)user.get("average_stars");
                statement.setDouble(8,average_stars);
             
                String type=(String)user.get("type").toString();
                statement.setString(9,type);
             
                String compliments=(String)user.get("compliments").toString();
                statement.setString(10,compliments);
              
                String elite=(String)user.get("elite").toString();
                statement.setString(11,elite);
             
                statement.executeUpdate();
                line= reader.readLine();
            }
            
            Statement s = C.createStatement();
            String sql="SELECT * FROM yelp_user WHERE ROWNUM <= 10";
            
            ResultSet resultSet=s.executeQuery(sql);
            while(resultSet.next()){
                System.out.println("\n"+resultSet.getString(1) + "\t"+resultSet.getString(2)+ "\t"+resultSet.getLong(3)+ "\t"+resultSet.getString(4)+ "\t"+resultSet.getString(5)+ "\t"+resultSet.getString(6)+ "\t"+resultSet.getLong(7)+ "\t"+resultSet.getLong(8)+ "\t"+resultSet.getString(9)+ "\t"+resultSet.getString(10)+ "\t"+resultSet.getString(11));
                
            }
            
            
        }
        catch(Exception E){
            System.out.println("SQLException:"+E.getMessage());
        }
    }   

    private void insertbusiness(Connection C){
        JSONParser jsonParser = new JSONParser();
        String fileName = "/Users/Pranav/Desktop/santa clara/Winter2020/Database_Systems/Assignment/A3/YelpDataset-CptS451/yelp_business.json";
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException cnfe){
            System.out.println("Expeption:"+cnfe);
        }
        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            
            PreparedStatement statement = C.prepareStatement("Insert into yelp_business(business_id,full_address,hours,open,categories,city,review_count,name,neighborhoods,longitude,state,stars,latitude,attributes,type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            String  line= reader.readLine();
            System.out.println("business)");
            while (line != null ) {
                Object obj = jsonParser.parse(line);
                JSONObject business = (JSONObject) obj;
                String business_id=(String)business.get("business_id");
                statement.setString(1,business_id);
                
                String full_address=(String)business.get("full_address");
                statement.setString(2,full_address);
                
                String hours=(String)business.get("hours").toString();
                statement.setString(3,hours);
                
                String open=(String)business.get("open").toString();
                statement.setString(4,open);
                
                String categories=(String)business.get("categories").toString();
                statement.setString(5,categories);
                
                String city=(String)business.get("city");
                statement.setString(6,city);
                
                Long review_count=(Long)business.get("review_count");
                statement.setLong(7, review_count);
                
                String name=(String)business.get("name");
                statement.setString(8,name);
                
                String neighborhoods=(String)business.get("neighborhoods").toString();
                statement.setString(9,neighborhoods);
                
                Double longitude=(Double)business.get("longitude");
                statement.setDouble(10,longitude);
                
                String state=(String)business.get("state");
                statement.setString(11,state);
                
                Double stars=(Double)business.get("stars");
                statement.setDouble(12,stars);
                
                Double latitude=(Double)business.get("latitude");
                statement.setDouble(13,latitude);
                
                String attributes=(String)business.get("attributes").toString();
                statement.setString(14,attributes);
                
                String type=(String)business.get("type");
                statement.setString(15,type);
                
                statement.executeUpdate();
                line= reader.readLine();
            }
            Statement s = C.createStatement();
            String sql="SELECT * FROM yelp_business WHERE ROWNUM <= 10";
            ResultSet resultSet=s.executeQuery(sql);
            while(resultSet.next()){
                System.out.println("\n"+resultSet.getString(1) + "\t"+resultSet.getString(2)+ "\t"+resultSet.getString(3)+ "\t"+resultSet.getString(4)+ "\t"+resultSet.getString(5)+ "\t"+resultSet.getString(6)+ "\t"+resultSet.getInt(7)+ "\t"+resultSet.getString(8)+ "\t"+resultSet.getString(9)+ "\t"+resultSet.getString(10)+ "\t"+resultSet.getString(11)+ "\t"+resultSet.getInt(12)+ "\t"+resultSet.getString(13)+ "\t"+resultSet.getString(14)+ "\t"+resultSet.getString(15));
                
            }
        }
        catch(Exception E){
            System.out.println("SQLException:"+E.getMessage());
        }
        
    }


    private void insertreview(Connection C){
        JSONParser jsonParser = new JSONParser();
        String fileName = "/Users/Pranav/Desktop/santa clara/Winter2020/Database_Systems/Assignment/A3/YelpDataset-CptS451/yelp_review.json";
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException cnfe){
            System.out.println("Expeption:"+cnfe);
        }
        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            
            PreparedStatement statement = C.prepareStatement("Insert into yelp_review(votes,user_id,review_id,stars,rdate,type,business_id,text) values(?,?,?,?,to_date(?,'YYYY-MM-DD'),?,?,?)");
            String  line= reader.readLine();
            int count=0;
            System.out.println("review");
            while (line != null ) {
                count++;
                if((count%10000)==0){
                    System.out.println(count);
                }
                
                    Object obj = jsonParser.parse(line);
                    JSONObject review = (JSONObject) obj;
                    String votes=(String)review.get("votes").toString();
                    statement.setString(1,votes);
                    
                    
                    String user_id=(String)review.get("user_id");
                    statement.setString(2,user_id);
                    
                    String review_id=(String)review.get("review_id");
                    statement.setString(3,review_id);
                    
                    Long stars=(Long)review.get("stars");
                    statement.setLong(4,stars);
                    
                    String rdate=(String)review.get("date").toString();
                    statement.setString(5,rdate);
                    
                    
                    String type=(String)review.get("type");
                    statement.setString(6,type);
                    
                    String business_id=(String)review.get("business_id").toString();
                    statement.setString(7,business_id);
                    
                    String text=(String)review.get("text").toString();
                    statement.setString(8,text);
                    statement.executeUpdate();
                
                line= reader.readLine();
            }
            Statement s = C.createStatement();
            String sql="SELECT * FROM yelp_review WHERE ROWNUM <= 10";
            ResultSet resultSet=s.executeQuery(sql);
            while(resultSet.next()){
                System.out.println("\n"+resultSet.getString(1) + "\t"+resultSet.getString(2)+ "\t"+resultSet.getString(3)+ "\t"+resultSet.getLong(4)+ "\t"+resultSet.getString(5)+ "\t"+resultSet.getString(6)+ "\t"+resultSet.getString(7)+"\t"+resultSet.getString(8));
                
            }
        }
        catch(Exception E){
            System.out.println("SQLException:"+E.getMessage());
        }
        
    }
    
    

    private void insertcheckin(Connection C){
        JSONParser jsonParser = new JSONParser();
        String fileName = "/Users/Pranav/Desktop/santa clara/Winter2020/Database_Systems/Assignment/A3/YelpDataset-CptS451/yelp_checkin.json";
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException cnfe){
            System.out.println("Expeption:"+cnfe);
        }
        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            
            PreparedStatement statement = C.prepareStatement("Insert into yelp_checkin(checkin_info,type,business_id) values(?,?,?)");
            String  line= reader.readLine();
            System.out.println("Checkin");
            while (line != null ) {
                Object obj = jsonParser.parse(line);
                JSONObject review = (JSONObject) obj;
                String checkin_info=(String)review.get("checkin_info").toString();
                statement.setString(1,checkin_info);
                
                
                String type=(String)review.get("type");
                statement.setString(2,type);
                
                String business_id=(String)review.get("business_id");
                statement.setString(3,business_id);
                
                
                statement.executeUpdate();
                line= reader.readLine();
            }
            Statement s = C.createStatement();
            String sql="SELECT * FROM yelp_checkin WHERE ROWNUM <= 10";
            ResultSet resultSet=s.executeQuery(sql);
            while(resultSet.next()){
                System.out.println("\n"+resultSet.getString(1) + "\t"+resultSet.getString(2)+ "\t"+resultSet.getString(3));
                
            }
        }
        catch(Exception E){
            System.out.println("SQLException:"+E.getMessage());
        }
        
    }
    
    
    
    private void userDat(Connection C){
        String fileName = "/Users/Pranav/Desktop/santa clara/Winter2020/Database_Systems/Assignment/A3/YelpDataset-CptS451/yelp_user.json";
        JSONParser jsonParser = new JSONParser();
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException cnfe){
            System.out.println("Expeption:"+cnfe);
        }
        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            
            PreparedStatement statement = C.prepareStatement("Insert into userCount(user_id,nfriends,nvotes) values(?,?,?)");
            
            String  line= reader.readLine();
            int count=1;
            while (line != null ) {
                int vote=0;
                int friend=0;
                System.out.println(count);
                Object obj = jsonParser.parse(line);
                JSONObject user = (JSONObject) obj;
                
                
                String user_id=(String)user.get("user_id").toString();
                statement.setString(1,user_id);
                
                String friends=(String)user.get("friends").toString();
                if(friends.length()>2){
                    friends.substring(1, friends.length()-1);
                    String[] arrOfStr = friends.split(",");
                    
                    friend=arrOfStr.length;
                    
                        
                }
                
                statement.setInt(2,friend);
                
                Map votes = ((Map)user.get("votes"));
                
                // iterating address Map
                Iterator<Map.Entry> itr1 = votes.entrySet().iterator();
                while (itr1.hasNext()) {
                    Map.Entry pair = itr1.next();
                    long ipInt = ((Number) (pair.getValue())).longValue();
                    vote=vote+(int)ipInt;
                } 
                
                
                statement.setInt(3,vote);
                
                
                
                statement.executeUpdate();
                line= reader.readLine();
                count++;
            }
            
            Statement s = C.createStatement();
            String sql="SELECT * FROM userCount WHERE ROWNUM <= 10";
            
            ResultSet resultSet=s.executeQuery(sql);
            while(resultSet.next()){
                System.out.println("\n"+resultSet.getString(1) + "\t"+resultSet.getInt(2)+ "\t"+resultSet.getInt(3));
                
            }
            
            
        }
        catch(Exception E){
            System.out.println("SQLException:"+E.getMessage());
        }
    }
    
    private void insertcat(Connection C){
       
        JSONParser jsonParser = new JSONParser();
        
        ArrayList<String> main_categories = new ArrayList<String>();
        main_categories.add("Active Life");
        main_categories.add("Arts & Entertainment");
        main_categories.add("Automotive");
        main_categories.add("Car Rental");
        main_categories.add("Cafes");
        main_categories.add("Beauty & Spas");
        main_categories.add("Convenience Stores");
        main_categories.add("Dentists");
        main_categories.add("Doctors");
        main_categories.add("Drugstores");
        main_categories.add("Department Stores");
        main_categories.add("Education");
        main_categories.add("Event Planning & Services");
        main_categories.add("Flowers & Gifts");
        main_categories.add("Food");
        main_categories.add("Health & Medical");
        main_categories.add("Home Services");
        main_categories.add("Home & Garden");
        main_categories.add("Hospitals");
        main_categories.add("Hotels & Travel");
        main_categories.add("Hardware Stores");
        main_categories.add("Grocery");
        main_categories.add("Medical Centers");
        main_categories.add("Nurseries & Gardening");
        main_categories.add("Nightlife");
        main_categories.add("Restaurants");
        main_categories.add("Shopping");
        main_categories.add("Transportation");
        String fileName = "/Users/Pranav/Desktop/santa clara/Winter2020/Database_Systems/Assignment/A3/YelpDataset-CptS451/yelp_business.json";
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException cnfe){
            System.out.println("Expeption:"+cnfe);
        }
        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            
            PreparedStatement statement = C.prepareStatement("Insert into cat(business_id,category,sub_category) values(?,?,?)");
            int count=0;
            String  line= reader.readLine();
            while (line != null ) {
                ArrayList<String> categories = new ArrayList<String>();
                ArrayList<String> sub_categories = new ArrayList<String>();
                
                Object obj = jsonParser.parse(line);
                JSONObject business = (JSONObject) obj;
                
                String business_id=(String)business.get("business_id");
                statement.setString(1,business_id);
               
                JSONArray category=(JSONArray)business.get("categories");
                Iterator<String> iterator = category.iterator();
                
                
                while(iterator.hasNext()) {
                    String it=iterator.next();
                    
                    
                    if(main_categories.contains(it)){
                        
                        categories.add(it);
                    }
                    else{
                        sub_categories.add(it);
                    }
                }
                String categorym=(String)categories.toString();
                statement.setString(2,categorym);
                String subc=(String)sub_categories.toString();
                statement.setString(3,subc);
                
                statement.executeUpdate();
                line= reader.readLine();
            }
            Statement s = C.createStatement();
            String sql="SELECT * FROM cat WHERE ROWNUM <= 10";
            ResultSet resultSet=s.executeQuery(sql);
            while(resultSet.next()){
                System.out.println("\n"+resultSet.getString(1) + "\t"+resultSet.getString(2)+ "\t"+resultSet.getString(3));
            }
        }
        catch(Exception E){
            System.out.println("SQLException:"+E.getMessage());
        }
    }
    
}


