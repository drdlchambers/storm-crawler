package com.chambers;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;

import java.util.Map;


public class ScrapeUrl extends ShellBolt implements IRichBolt {

    public ScrapeUrl() {
      super("python", "scrapeurl.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("child_url","parent_url","domain","child_depth"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }