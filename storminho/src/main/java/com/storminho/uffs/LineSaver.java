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

public class LineSaver extends BaseBasicBolt implements Serializable{
    
    OutputCollector _collector;
    
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }    
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        JedisPool pool = new JedisPool("127.0.0.1");
        Jedis jedis = pool.getResource();
        String key = tuple.getString(0).split(Variables.splitChars)[Variables.fieldId];
        String line = tuple.getString(0);
        
        //SETNX = apenas seta a key se ela já não existir no redis, se existir add +value
        //Salva no formato [chave = rec0102][toda a linha incluindo o rec]
        jedis.set(key, line);
        //Utilizar  Long list se desejar testar
        //System.out.println(jedis.get(key));
    }
     @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lineId"));
    }
}