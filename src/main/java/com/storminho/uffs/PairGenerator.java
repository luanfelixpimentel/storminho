package com.storminho.uffs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.storm.tuple.Values;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import static redis.clients.jedis.Protocol.Command.CLIENT;

public class PairGenerator extends BaseRichBolt implements IRichBolt{
    
    private JedisPool pool;
    private OutputCollector collector;
    
    private void setupJedisPool() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setMaxTotal(1);
    poolConfig.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(60));

    pool = new JedisPool(poolConfig,"localhost", 6379);
    }
    
    @Override
    public void execute(Tuple tuple) {
     String word = tuple.getString(0);
     String lineId = tuple.getString(1);
     String aux;
     Jedis jedis = pool.getResource();
     JedisCommands jedisCommands = null;
    
     Long listCount = jedis.llen(word);
     //foi necessário o uso do listCount para o lrange
     List<String> list = jedis.lrange(word, 0 ,listCount);
     for(int i=0; i<list.size(); i++) {
       //if(list.get(i).equals(lineId)){
       // System.out.println("mesmo registro comparado: ignorado");
            aux= list.get(i);
            collector.emit(new Values(aux, lineId));
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "lineId"));
    }
    
     @Override
    public void cleanup() {
    }
    
    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        collector = collector;
    }
}