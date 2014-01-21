package com.chambers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ExtractorBolt extends BaseRichBolt {

    OutputCollector outputCollector;
    Map<String,String> config;
    Integer msThrottle;		// Minimum wait between pulling next page on a given domain
    
    // Tracks the last hit crawl time per domain
    public static Map<String, Long> speedTracker = new HashMap<String, Long>();

   

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        config = (Map<String,String>) map;
        msThrottle = 1000;
        
    }
    
    
    public static String returnRawHTML(String webUrl) throws IOException {
		  
	  	URL url = new URL(webUrl);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("GET");
        con.connect();
        
        String str = "";

        int code = con.getResponseCode();
        if (code==200) {
            
        	  Pattern p = Pattern.compile("text/html;\\s+charset=([^\\s]+)\\s*");
	  		  Matcher m = p.matcher(con.getContentType());
	  		  // If Content-Type doesn't match this pre-conception, choose default and 
	  		  // hope for the best.
	  		  String charset = m.matches() ? m.group(1) : "ISO-8859-1";
	  		  Reader r = new InputStreamReader(con.getInputStream(), charset);
	  		  StringBuilder buf = new StringBuilder();
	  		  while (true) {
	  		    int ch = r.read();
	  		    if (ch < 0)
	  		      break;
	  		    buf.append((char) ch);
	  		  }
	  		  str = buf.toString();	  
        	
        } 
        
        return str;
	  
  }

    

    @Override
    public void execute(Tuple input) {
    	  Utils.sleep(1000);
		    
		    String currentULR = input.getStringByField("current_url");
		    String currentDomain = input.getStringByField("current_domain");
		    Integer currentDepth = input.getIntegerByField("current_depth");
		    
			// Check if we should slow down
	        Long lastCrawl = speedTracker.get(currentDomain);
	        long now = System.currentTimeMillis();
	        if (lastCrawl != null && (now - lastCrawl) < msThrottle) {
	            Utils.sleep(now - lastCrawl);
	        }
		    
		    try {
				String rawHTML = returnRawHTML(currentULR);
				speedTracker.put(currentDomain, System.currentTimeMillis());
				
				if (rawHTML != "") {
					System.out.println(">>>>>>>>>>>>>>>>>" + "HTML being sent out: " + currentULR);
					outputCollector.emit(new Values(rawHTML,currentULR,currentDomain,currentDepth));
				} else {
					return;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
	    
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("raw_html","current_url","current_domain","current_depth"));
    }
}
