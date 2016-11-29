package com.storminho.uffs;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
 import weka.classifiers.trees.J48;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.task.TopologyContext;
import weka.core.DenseInstance;
import weka.core.Instances;

public class Arvore extends BaseRichBolt implements IRichBolt {
    OutputCollector _collector;
    J48 arv;
    Instances data;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        data = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(Variables.arffPath + Variables.trainingOutputFile));
            data = new Instances(reader);
            reader.close();
            data.setClassIndex(data.numAttributes() - 1);
        } catch (Exception e) {
            System.out.println("\n\n" + e + "\n\n");
        }

        String[] opt = new String[1];
        // opt[0] = "-U";
        arv = new J48();
        try {
            // arv.setOptions(opt);
            arv.buildClassifier(data);
        } catch (Exception e) { System.out.println("\n\n" + e + "\n\n"); }
    }

    @Override
    public void execute(Tuple tuple) {
        Instances ins = (Instances)tuple.getValues().get(0);
        double result;
        try {
            result = arv.classifyInstance(ins.instance(0));
            System.out.println("O que deu : " + result);
        } catch (Exception ex) {
            Logger.getLogger(Arvore.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("O que veio: " + (double)tuple.getInteger(1) + "\n");

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("resposta_arvore", "resposta_certa"));
    }

    @Override
    public void cleanup() {

    }
}
