package com.storminho.uffs;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.storm.tuple.Values;
import weka.core.DenseInstance;
import weka.core.Instances;

public class DecisionTree extends BaseRichBolt implements IRichBolt {
    OutputCollector _collector;
    J48 arv;
    Instances data;
    BufferedReader reader;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        //weka
        data = null;
        try {
            reader = new BufferedReader(new FileReader(Variables.arffPath + Variables.trainingOutputFile));
            data = new Instances(reader);
            data.setClassIndex(data.numAttributes() - 1);
            arv = new J48();
            arv.setUnpruned(true);
            arv.buildClassifier(data);
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        DenseInstance ins = (DenseInstance)tuple.getValues().get(0);
        double result = -1;
        ins.setDataset(data);
        ins.setClassMissing();
        try {
            result = arv.classifyInstance(ins);
            //System.out.println(ins + "\n" + "O que deu : " + result + " e o que tinha que dar " + tuple.getInteger(1));
            //System.out.println();
        } catch (Exception ex) {
            System.out.println(ex);
            Logger.getLogger(DecisionTree.class.getName()).log(Level.SEVERE, null, ex);
        }
        if ((int)(result + 0.5) != (int)result) {
            System.out.print("//////////////////////////////////\n\n");
            System.out.print("/Resposta    : " + result);
            System.out.print("/Cast com 0.5: " + (int)(result + 0.5));
            System.out.print("/Cast normal : " + (int)result);
            System.out.print("//////////////////////////////////\n\n");

        }
        _collector.emit(new Values((int)(result + 0.5), tuple.getInteger(1)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("resposta_arvore", "resposta_certa"));
    }

    @Override
    public void cleanup() {
        try {
            reader.close();
        } catch (IOException ex) {
            Logger.getLogger(DecisionTree.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
