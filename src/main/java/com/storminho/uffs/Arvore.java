package com.storminho.uffs;


import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
 import weka.classifiers.trees.J48;

import java.util.Map;
import org.apache.storm.task.TopologyContext;
import weka.core.Instances;

public class Arvore extends BaseRichBolt implements IRichBolt {
    OutputCollector _collector;
    J48 arv;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        Instances data = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(Variables.arffPath + Variables.trainingOutputFile));
            data = new Instances(reader);
            reader.close();
            data.setClassIndex(data.numAttributes() - 1);
        } catch (Exception e) {
            System.out.println("\n\n" + e + "\n\n");
        }

        String[] opt = new String[1];
        opt[0] = "-U";
        arv = new J48();
        try {
            arv.setOptions(opt);
            arv.buildClassifier(data);
        } catch (Exception e) { System.out.println("\n\n" + e + "\n\n"); }
    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

    @Override
    public void cleanup() {

    }
}
