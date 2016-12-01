package com.storminho.uffs;

<<<<<<< HEAD

=======
>>>>>>> d4efcee6ffd6085bdb4975175ee746df52c7bf93
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;
<<<<<<< HEAD

=======
>>>>>>> d4efcee6ffd6085bdb4975175ee746df52c7bf93

import org.apache.storm.tuple.Values;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
<<<<<<< HEAD

import org.apache.storm.topology.base.BaseRichBolt;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
=======
import org.apache.storm.topology.base.BaseRichBolt;
import redis.clients.jedis.JedisPool;
>>>>>>> d4efcee6ffd6085bdb4975175ee746df52c7bf93

public class PairGenerator extends BaseRichBolt implements Serializable{
    
    private OutputCollector _collector;
    
    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        Jedis jedis = null;
    }


    @Override
    public void execute(Tuple tuple) {
        
     String word = tuple.getString(0);
     String lineId = tuple.getString(1);
     String aux;

     
     JedisPool pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");
     Jedis jedis = pool.getResource();
     
     Long listCount = jedis.llen(word);
     //foi necessário o uso do listCount para o lrange
     List<String> list = jedis.lrange(word, 0 ,listCount);
        for(int i=0; i<list.size(); i++) {
        //if(list.get(i).equals(lineId)){
        // System.out.println("mesmo registro comparado: ignorado");
        aux= list.get(i);
        //System.out.println("Stored string in redis:: "+list.get(i));
        _collector.emit(new Values(aux, lineId));
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("line1", "line2"));
    }
}