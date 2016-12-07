package com.storminho.uffs;

import redis.clients.jedis.Jedis;

import java.util.Map;
import org.apache.storm.task.OutputCollector;


import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Values;


public class WordIndexSave extends BaseRichBolt implements IRichBolt {
    
    OutputCollector _collector;
    Jedis jedis;
    //JedisPool pool;
    
    @Override
    public void prepare(Map map, TopologyContext context,  OutputCollector collector) {
       _collector = collector;
         try {
            jedis = new Jedis("localhost");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
       // pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");
       // jedis = pool.getResource();
       // jedis.bgsave();
        String word = tuple.getString(0);
        String lineId = tuple.getString(1);

        try {
            if (!word.matches("^\\s+$")) { //ignores all empty or only-spaces words
                jedis.sadd(word, lineId); //method to add a value only if he doesn't exist in the set yet
                _collector.emit(new Values(word, lineId));

                // This bit of code is to check which words are being saved and their values
                // System.out.println("Palavra salva " + word);
                // Set<String> set = jedis.smembers(word);
                // for (String aux : set) { System.out.print(aux + ", "); }
                // System.out.println();
              
            }
        } catch(Exception e) {
            e.printStackTrace();
            
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }

    @Override
    public void cleanup() {
    }
}
