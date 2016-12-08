package edu.uffs.storminho.bolts;

import edu.uffs.storminho.Variables;
import redis.clients.jedis.Jedis;

import java.util.Map;
import org.apache.storm.task.OutputCollector;


import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Values;


public class WordIndexSaveBolt extends BaseRichBolt implements IRichBolt {

    OutputCollector _collector;
    Jedis jedis;

    @Override
    public void prepare(Map map, TopologyContext context,  OutputCollector collector) {
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

        try {
            if (!word.matches("^\\s+$") && jedis.scard(word) < Variables.MAX_SET_SIZE) { //ignores all empty or only-spaces words
                jedis.sadd(word, lineId); //method to add a value only if he doesn't exist in the set yet
                _collector.emit(new Values(word, lineId));

                // This bit of code is to check which words are being saved and their values
                // System.out.println("Palavra salva " + word);
                // Set<String> set = jedis.smembers(word);
                // for (String aux : set) { System.out.print(aux + ", "); }
                // System.out.println();

            }
        } catch(Exception e) {
            e.printStackTrace();

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("Palavra", "ID"));
    }

    @Override
    public void cleanup() {
    }
}
