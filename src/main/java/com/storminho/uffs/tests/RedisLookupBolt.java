package com.storminho.uffs.tests;

import java.util.List;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.concurrent.TimeUnit;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Map;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisLookupBolt extends BaseRichBolt implements IRichBolt{

      private JedisPool pool;

    private void setupJedisPool() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setMaxTotal(1);
    poolConfig.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(60));

    pool = new JedisPool(poolConfig,"localhost", 6379);
    }

    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        collector = collector;
    }

    public void execute(Tuple tuple) {
        Jedis jedis = pool.getResource();
        JedisCommands jedisCommands = null;
        String word = tuple.getString(0);
        String lineId = tuple.getString(1);
        Long listCount = jedis.llen(word);
        //foi necessário o uso do listCount para o lrange
        List<String> list = jedis.lrange(word, 0 ,listCount);
        for(int i=0; i<list.size(); i++)
        System.out.println("Stored string in redis:: "+list.get(i));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // declarer.declare(new Fields(""));
    }

    public void cleanup() {

    }
}
