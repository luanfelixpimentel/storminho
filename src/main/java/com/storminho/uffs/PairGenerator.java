package com.storminho.uffs;

import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Values;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.TopologyContext;

public class PairGenerator extends BaseBasicBolt{
    
    private JedisPool pool;
    
    private void setupJedisPool() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setMaxTotal(1);
    poolConfig.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(60));

    pool = new JedisPool(poolConfig,"localhost", 6379);
    }
    
    public void execute(String word, String lineId) {
    Jedis jedis = pool.getResource();
    JedisCommands jedisCommands = null;
    
    jedis.;
    if("");
    }
   }


 @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "id"));
  }
}