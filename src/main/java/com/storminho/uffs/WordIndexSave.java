package com.storminho.uffs;

import java.io.Serializable;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Set; //necessary to debug


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class WordIndexSave extends BaseBasicBolt implements Serializable {
    JedisPool pool;
    Jedis jedis;

    @Override
    public void prepare(Map map, TopologyContext context) {
        pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");
        jedis = pool.getResource();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        String lineId = tuple.getString(1);


        try {
            if (!word.matches("^\\s+$")) { //ignores all empty or only-spaces words
                jedis.sadd(word, lineId); //method to add a value only if he doesn't exist in the set yet
                collector.emit(new Values(word, lineId));

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
        pool.close();
    }
}
