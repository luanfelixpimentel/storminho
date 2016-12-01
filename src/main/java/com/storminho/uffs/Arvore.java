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
import weka.core.DenseInstance;
import weka.core.Instances;

public class Arvore extends BaseRichBolt implements IRichBolt {
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
            System.out.println("checkcheckpizza1" + reader);
            data = new Instances(reader);
            System.out.println("checkcheckpizza2" + data);
            data.setClassIndex(data.numAttributes() - 1);
            System.out.println("checkcheckpizza3");
            arv = new J48();
            System.out.println("checkcheckpizza4");
            arv.setUnpruned(true);
            System.out.println("checkcheckpizza5");
            arv.buildClassifier(data);
            System.out.println("checkcheckpizza6");
        } catch (Exception ex) {
            System.out.println(ex);
        }

    }

    @Override
    public void execute(Tuple tuple) {
        DenseInstance ins = (DenseInstance)tuple.getValues().get(0);
        double result;
        ins.setDataset(data);
        ins.setClassMissing();
        System.out.println(data.checkInstance(ins));
        try {
            result = arv.classifyInstance(ins);
            System.out.println("O que deu : " + result + " e o que tinha que dar " + tuple.getInteger(1));
            System.out.println();
        } catch (Exception ex) {
            System.out.println(ex);
            Logger.getLogger(Arvore.class.getName()).log(Level.SEVERE, null, ex);
        }

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
            Logger.getLogger(Arvore.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}