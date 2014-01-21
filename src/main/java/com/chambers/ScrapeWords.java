package com.chambers;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;

import java.util.Map;


public class ScrapeWords extends ShellBolt implements IRichBolt {

    public ScrapeWords() {
      super("python", "scrapewords.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word","count","current_page","domain"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }