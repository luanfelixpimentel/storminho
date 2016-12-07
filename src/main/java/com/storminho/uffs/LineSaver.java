package com.storminho.uffs;

import java.util.Map;

import redis.clients.jedis.Jedis;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;


public class LineSaver extends BaseRichBolt implements IRichBolt{

    OutputCollector _collector;
    Jedis jedis;
    //JedisPool pool;
    
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
       _collector = collector;
        try {
            jedis = new Jedis("localhost");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        //  pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");
       // jedis = pool.getResource();
       // jedis.bgsave();
        String key = tuple.getString(0).split(Variables.SPLIT_CHARS)[Variables.FIELD_ID];
        String line = tuple.getString(0);
        
        //Salva no formato [chave = rec0102][toda a linha incluindo o rec]
        if(!key.equals("")) {
            jedis.set(key, line);
            String linha = jedis.get(key);
         //   pool.close();
        }
        //  System.out.println("JEDIS"+linha);
       // jedis.shutdown();
    }
     @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lineId"));
    }
    
    @Override
    public void cleanup() {
        //pool.close();
        //pool.destroy();
    }
}
