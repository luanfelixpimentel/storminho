package com.storminho.uffs;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;

import org.apache.storm.tuple.Values;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.base.BaseRichBolt;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class PairGenerator extends BaseRichBolt implements Serializable{
    
    private OutputCollector _collector;
    JedisPool pool;

    
    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        Jedis jedis = null;
        pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");
    }


    @Override
    public void execute(Tuple tuple) {
     Jedis jedis = pool.getResource();  
     String chega = tuple.getString(1);
     if(!tuple.getString(0).isEmpty()){
     List<String> list = jedis.lrange(tuple.getString(0), 0 ,jedis.llen(tuple.getString(0)));
     for (String aux : list) {
//         System.out.println(jedis.get(aux) + " \n" + jedis.get(chega) + "\n");
         _collector.emit(new Values(jedis.get(aux), jedis.get(chega)));
     }
     }
     //jedis.shutdown();
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("line1", "line2"));
    }
}