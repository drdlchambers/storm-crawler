package com.chambers;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.*;


public class UrlQueueSpout3 extends BaseRichSpout {
	
	 public static Map<String, String> CRAWL_HISTORY = new HashMap<String, String>();
	 
	 public static Map<String, Integer> CRAWL_DEPTH = new HashMap<String, Integer>() {{
		    put("http://www.uci.edu", 1);
		    put("http://www.ucr.edu", 1);
		    put("http://www.ucla.edu", 1);
		    put("http://www.fresnostate.edu", 1);
	 }};
	  
	 public static Map<String, String> URL_QUEUE = new LinkedHashMap<String, String>() {{
		    put("http://www.uci.edu", "uci.edu");
		    put("http://www.ucr.edu", "ucr.edu");
		    put("http://www.ucla.edu", "ucla.edu");
		    put("http://www.fresnostate.edu", "fresnostate.edu");
	 }};
	
		  
	 public static Integer maxCrawlDepth = 10;
	 
	 
	 SpoutOutputCollector _collector;
	  
	 
	    
	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	  }
	 

	  @Override
	  public void nextTuple() {
		  Utils.sleep(200);
		    
		    String nextULR = "";
		    String nextDomain = "";
		    Integer nextDepth = 1;
		    
		    Iterator<Map.Entry<String,String>> it = URL_QUEUE.entrySet().iterator();
		    
		    if (it.hasNext()) {
		    	Map.Entry<String, String> entry = it.next();

		    	nextULR = entry.getKey();
		    	nextDomain = entry.getValue();
		    	Integer depth = CRAWL_DEPTH.get(nextULR);
		    	if (depth != null) {
		    		nextDepth = depth;
		    	}
		    		
		    	// Remove Entry from Queue
		    	it.remove();
		    } else {
		    	// Out-ran Queue -- wait 3 seconds for more URLS to be populated
		         Utils.sleep(3000);
		         return;
		    }
			
		    if (nextDepth <= maxCrawlDepth) {
		    	CRAWL_HISTORY.put(nextULR, nextDomain);
		    	System.out.println("~~~~~~~~~~~~~~~~~~~~" + "URL being sent out: " + nextULR);
		    	_collector.emit(new Values(nextULR,nextDomain,nextDepth));
		    } else {
		    	return;
		    }
				
	    
	  }

	  @Override
	  public void ack(Object id) {
	  }

	  @Override
	  public void fail(Object id) {
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("current_url","current_domain","current_depth"));
	  }

  
}
