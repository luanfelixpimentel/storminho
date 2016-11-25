package com.storminho.uffs;

import java.util.List;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.*;
import org.apache.storm.topology.OutputFieldsDeclarer;


public class LineSaver extends BaseBasicBolt{

    private HashMap<Object, Object> indexes;
    private JedisPool pool;

    @Override
    public void prepare(Map map, TopologyContext context) {
        this.indexes = new HashMap<>();
    }
    
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
    Jedis jedis = pool.getResource();
    JedisCommands jedisCommands = null;
    //Jedis will store on database the Key (ex: rec-01234, and the rest of the string attached as line
    jedis.lpush(tuple.getString(0));
   }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}