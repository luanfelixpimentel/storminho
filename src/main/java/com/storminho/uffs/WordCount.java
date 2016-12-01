package com.storminho.uffs;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

// import org.apache.logging.log4j.Logger;
// import org.apache.logging.log4j.LogManager;

public class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Long lineId = tuple.getLong(1);
      Integer count = counts.get(word);
      count = (count == null? 0:count+1);
      counts.put(word, count);
      collector.emit(new Values(word, count));
    //   System.out.println("Palavra, contagem e linha de origem: [" + word + "] [" + count + "] [" + lineId + "]");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }
