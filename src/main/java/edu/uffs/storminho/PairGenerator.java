package edu.uffs.storminho;

import java.util.Set;
import java.util.Map;

import org.apache.storm.tuple.Values;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class PairGenerator extends BaseRichBolt implements IRichBolt{

    OutputCollector _collector;
    Jedis jedis;
    //JedisPool pool;


    @Override
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
        //JedisPool pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");
       // jedis = pool.getResource();
       // jedis.bgsave();
        String word = tuple.getString(0);
        String lineId = tuple.getString(1);
        try {
            Set<String> set = jedis.smembers(word);
            for (String toPair : set) {
                 _collector.emit(new Values(jedis.get(toPair), jedis.get(lineId)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("line1", "line2"));
    }

    @Override
    public void cleanup() {
    }
}
