package com.chambers;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.chambers.ScrapeWords;
import com.chambers.WordWriter;
import com.chambers.UrlWriter;
import com.chambers.ScrapeUrl;
import com.chambers.ExtractorBolt;
import com.chambers.UrlQueueSpout3;


public class TopologyRunner {
	
	 static StormTopology createTopology() {
	        TopologyBuilder builder = new TopologyBuilder();

	        // Emits the URLs we want to scrape
	        builder.setSpout("url_queue_spout", new UrlQueueSpout3(), 1);

	        // Raw HTML Extractor
	        builder.setBolt("html_extractor_bolt", new ExtractorBolt(), 2)
	                .fieldsGrouping("url_queue_spout", new Fields("current_domain"));
	        
	        // Scrape Word Count
	        builder.setBolt("word_scraper_bolt", new ScrapeWords(), 2)
	                .shuffleGrouping("html_extractor_bolt");
	        
	        // Scrape URLs
	        builder.setBolt("url_scraper_bolt", new ScrapeUrl(), 2)
            .shuffleGrouping("html_extractor_bolt");
	        
	        // Write Words to PostgreSQL
	        builder.setBolt("write_words_db", new WordWriter(), 2).shuffleGrouping("word_scraper_bolt");
	        
	        // Write URLs to PostgreSQL and HashMap
	        builder.setBolt("write_urls_db", new UrlWriter(), 2).shuffleGrouping("url_scraper_bolt");
	        
	        return builder.createTopology();
	        
	    }
	 

    public static void main(String args[]) {

        //Creates local storm cluster
        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();
        conf.setDebug(true);

        conf.setMaxTaskParallelism(Runtime.getRuntime().availableProcessors());
        conf.setDebug(false);

        cluster.submitTopology("storm-crawler", conf, createTopology());
        
        Utils.sleep(15000);
        cluster.killTopology("storm-crawler");
        cluster.shutdown();

    }

   

}
