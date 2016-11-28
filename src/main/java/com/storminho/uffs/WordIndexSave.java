package com.storminho.uffs;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;

public class WordIndexSave extends BaseBasicBolt {
    Map<String, Set> indexes;
    private OutputCollector collector;
    
    @Override
    public void prepare(Map map, TopologyContext context) {
        this.indexes = new HashMap<>();
    }
    
    private JedisPool pool;
    
    private void setupJedisPool() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setMaxTotal(1);
    poolConfig.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(60));

    pool = new JedisPool(poolConfig,"localhost", 6379);
    }
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      String lineId = tuple.getString(1);
      Set linesIndexes = this.indexes.get(word);
      
      Jedis jedis = pool.getResource();
      JedisCommands jedisCommands = null;
     
      //insert word and id into the set
      try {
          if (this.indexes.get(word) == null) {
              this.indexes.put(word, new TreeSet<String>());
              this.indexes.get(word).add(lineId);
              jedis.lpush(word, lineId);              
          }
          else {
            this.indexes.get(word).add(lineId);
            jedis.rpush(word, lineId);
          }
      } catch(Exception e) {
        }
      collector.emit(new Values(word, lineId));
    }   
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }

    @Override
    public void cleanup() {
        //print all the set before leave topology
        Iterator<Set> lt = this.indexes.values().iterator();
        Iterator<String> st = this.indexes.keySet().iterator();
        while (lt.hasNext()) {
            Iterator<String> it = lt.next().iterator();
            System.out.print("Palavra [" + st.next() + "]");
            while (it.hasNext()) {
                System.out.print(" " + it.next() +" ");
            }
            System.out.println();
        }
    }
  }
