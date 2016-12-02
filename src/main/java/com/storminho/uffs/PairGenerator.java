package com.storminho.uffs;

import java.io.Serializable;
import java.util.Set;
import java.util.Map;

import org.apache.storm.tuple.Values;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class PairGenerator extends BaseRichBolt implements Serializable{

    private OutputCollector _collector;
    JedisPool pool;
    Jedis jedis;


    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");
        jedis = pool.getResource();
    }


    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        String lineId = tuple.getString(1);
        try {
            Set<String> set = jedis.smembers(word);
            for (String toPair : set) {
                 _collector.emit(new Values(jedis.get(toPair), jedis.get(lineId), toPair, lineId));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("line1", "line2", "id1", "id2"));
    }

    @Override
    public void cleanup() {
        pool.close();
    }
}
