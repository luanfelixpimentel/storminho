package edu.uffs.storminho.bolts;


import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;

import java.util.Map;
import org.apache.storm.task.TopologyContext;

public class EmptyBolt extends BaseRichBolt implements IRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("", ""));
    }

    @Override
    public void cleanup() {

    }
}
