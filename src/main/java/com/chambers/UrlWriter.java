package com.chambers;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.chambers.UrlQueueSpout3;


public class UrlWriter extends BaseRichBolt {

    OutputCollector outputCollector;
    Map<String,String> config;
    Connection con;
    Statement st;
    String dbUrl;
    String user;
    String password;
    String stm;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        config = (Map<String,String>) map;
        dbUrl = "jdbc:postgresql://localhost/testdb";
        user = "user12";
        password = "Zw*93!";
        stm = "INSERT INTO url_map(domain,parent_url,child_url) VALUES (?, ?, ?)";
        
        try{
    	    Class.forName("org.postgresql.Driver");
    	    } catch (ClassNotFoundException cnfe){
    	      System.out.println("Could not find the JDBC driver!");
    	      System.exit(1);
    	    }
    }
    
    
    private void updateUrlList(String domain, String parentUrl, String childUrl) {
    	
    	PreparedStatement pst = null;
    	
    	try {
        	
            con = DriverManager.getConnection(dbUrl, user, password);
            
            pst = con.prepareStatement(stm);
            pst.setString(1, domain);
            pst.setString(2, parentUrl);
            pst.setString(3, childUrl);        
                             
            pst.executeUpdate();

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(WordWriter.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } finally {

            try {
                if (pst != null) {
                    pst.close();
                }
                if (con != null) {
                    con.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(WordWriter.class.getName());
                lgr.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
            
    	
    }
    
    
    private Boolean updateUrlQueue(String newUrl, String newDomain, Long newDepth) {
    	
    	// Check if New URL is already in the Queue or has already been crawled
    	if (!(UrlQueueSpout3.URL_QUEUE.containsKey(newUrl)) && !(UrlQueueSpout3.CRAWL_HISTORY.containsKey(newUrl))) {
    		
    		Integer tempDepth = (int) (long) newDepth;
    		
    		UrlQueueSpout3.URL_QUEUE.put(newUrl, newDomain);
    		UrlQueueSpout3.CRAWL_DEPTH.put(newUrl, tempDepth);
    		
    		return true;
    		
    	} else {
    		
    		return false;
    	}
    	
    }
    

    @Override
    public void execute(Tuple input) {
   
    	// "link","current_page","domain",child_depth
    	
    	 String parent_url = input.getStringByField("parent_url");
         String child_url = input.getStringByField("child_url");
         String domain = input.getStringByField("domain");
         Long child_depth = input.getLongByField("child_depth");
         
         
         if (updateUrlQueue(child_url, domain, child_depth)) {
        	 
        	 // System.out.println("~~~~~~~~~~~~~~~~~~~~" + "ParentUrl: " + parent_url + " - ChildUrl: " + child_url + " - Domain: " + domain);
        	 
        	 
             try {
          
            	updateUrlList(domain, parent_url, child_url);
             	outputCollector.ack(input);

             } catch (Throwable t) {

                 outputCollector.reportError(t);
                 outputCollector.fail(input);

             }
             
        	 
         }
         
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //No outputs
    }
}
