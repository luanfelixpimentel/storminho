package com.storminho.uffs;

import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.TimeUnit;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;


public class LineSaver extends BaseBasicBolt{

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
    Jedis jedis = pool.getResource();
    JedisCommands jedisCommands = null;
    //Jedis will store on database the Key (ex: rec-01234, and the rest of the string attached as list
    jedis.set(tuple.getString(0).split(Variables.splitChars)[Variables.fieldId], tuple.getString(0));
   }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
//        throw new UnsupportedOperationException("Line-saver não possui output fields"); //To change body of generated methods, choose Tools | Templates.
        ofd.declare(new Fields("oi"));
    }

    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        collector = collector;
    }

     @Override
    public void cleanup() {

    }
}
