package edu.uffs.storminho.bolts;

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

public class PairGeneratorBolt extends BaseRichBolt implements IRichBolt{

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
        String word = tuple.getString(0);
        String lineId = tuple.getString(1);
        String linha1 = jedis.get(lineId);
        String linha2;

        try {
            Set<String> set = jedis.smembers(word);
            for (String toPair : set) {
                linha2 = jedis.get(toPair);
                if (linha1 != null && linha2 != null)
                    _collector.emit(new Values(linha1, linha2));

                 //out test
                 // System.out.println("[pg] " + linha1 + "\n" + linha2 + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("Linha 1", "Linha 2"));
    }

    @Override
    public void cleanup() {
    }
}
