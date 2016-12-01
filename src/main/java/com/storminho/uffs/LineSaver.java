package com.storminho.uffs;

import java.io.Serializable;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
 
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import redis.clients.jedis.JedisPoolConfig;

public class LineSaver extends BaseBasicBolt implements Serializable{
    
    OutputCollector _collector;
    
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        Jedis jedis = null;
    }    
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");
        String key = tuple.getString(0).split(Variables.splitChars)[Variables.fieldId];
        String line = tuple.getString(0);
        Jedis jedis = pool.getResource(); 
        //Salva no formato [chave = rec0102][toda a linha incluindo o rec]
        jedis.set(key, line);
        String linha = jedis.get(key);
        //  System.out.println("JEDIS"+linha);
        //pool.destroy();
    }
     @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lineId"));
    }
}