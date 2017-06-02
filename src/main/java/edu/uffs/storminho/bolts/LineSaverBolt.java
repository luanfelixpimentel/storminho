package edu.uffs.storminho.bolts;

import edu.uffs.storminho.Variables;
import java.util.Map;

import redis.clients.jedis.Jedis;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;


public class LineSaverBolt extends BaseRichBolt implements IRichBolt{

    OutputCollector _collector;
    Jedis jedis;

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
        //Salva no formato [ID][Linha]
        jedis.set(tuple.getString(0).split(Variables.SPLIT_CHARS)[Variables.FIELD_ID], tuple.getString(0));
        long startTime = System.currentTimeMillis();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
    }
}
