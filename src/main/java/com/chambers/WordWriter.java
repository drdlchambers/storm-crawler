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


public class WordWriter extends BaseRichBolt {

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
        stm = "INSERT INTO word_count(domain,url,word,count) VALUES (?, ?, ?, ?)";
        
        try{
    	    Class.forName("org.postgresql.Driver");
    	    } catch (ClassNotFoundException cnfe){
    	      System.out.println("Could not find the JDBC driver!");
    	      System.exit(1);
    	    }
    }
    
    
    private void updateWordCount(String domain, String currentUrl, String word, Long count) {
    	
    	PreparedStatement pst = null;
    	
    	try {
        	
            con = DriverManager.getConnection(dbUrl, user, password);
            
            pst = con.prepareStatement(stm);
            pst.setString(1, domain);
            pst.setString(2, currentUrl);
            pst.setString(3, word);
            pst.setLong(4, count);        
                             
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
    
    

    @Override
    public void execute(Tuple input) {
   
    	 String current_page = input.getStringByField("current_page");
         String word = input.getStringByField("word");
         String domain = input.getStringByField("domain");
         Long count = input.getLongByField("count");
    	
         // System.out.println("~~~~~~~~~~~~~~~~~~~~" + "Word: " + word + " - page: " + current_page + " - domain: " + domain);
         
        try {

        	updateWordCount(domain, current_page, word, count);
        	outputCollector.ack(input);

        } catch (Throwable t) {

            outputCollector.reportError(t);
            outputCollector.fail(input);

        }
        
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //No outputs
    }
}
