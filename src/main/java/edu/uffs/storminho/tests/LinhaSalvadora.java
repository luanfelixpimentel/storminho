package edu.uffs.storminho.tests;


import edu.uffs.storminho.Variables;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;

import java.util.Map;
import org.apache.storm.task.TopologyContext;
import redis.clients.jedis.Jedis;

public class LinhaSalvadora extends BaseRichBolt implements IRichBolt {
    OutputCollector _collector;
    Jedis jedis;

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
        String line = tuple.getString(0);
        String key = line.split(Variables.SPLIT_CHARS)[Variables.FIELD_ID];

        if(!key.equals("")) {
            jedis.set(key, line);
            System.out.println("[ls2] " + key + "\n" + jedis.get(key));
            System.out.println();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

    @Override
    public void cleanup() {

    }
}
