/*
DecisionTreeBolt
Entrada: Uma Instance da biblioteca Weka
Saída: Classificação dessa Instance
Simples implementação de uma árvore de decisão do tipo J48 sem poda
*/

package edu.uffs.storminho.bolts;


import edu.uffs.storminho.Variables;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import weka.core.DenseInstance;
import weka.core.Instances;

public class DecisionTreeBolt extends BaseRichBolt implements IRichBolt {
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
            reader = new BufferedReader(new FileReader(Variables.ARFF_PATH + Variables.TRAININGSET_OUTPUT_FILE));
            data = new Instances(reader);
            data.setClassIndex(data.numAttributes() - 1);
            arv = new J48();
            arv.setUnpruned(false);
            arv.buildClassifier(data);
            reader.close();
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        DenseInstance instance = (DenseInstance)tuple.getValue(0);
        double result = -1;
        String linha1 = tuple.getString(1), linha2 = tuple.getString(2);

        instance.setDataset(data);
        //instance.setClassMissing();
        try {
            result = arv.classifyInstance(instance);
        } catch (Exception ex) {
            System.out.println(ex);
            Logger.getLogger(DecisionTreeBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
//        System.out.println("[dt]" + (int)(result + 0.5) + "\n" + linha1 + "\n" + linha2 + "\n");
        _collector.emit(new Values((int)(result + 0.5), linha1, linha2));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Rª. DecisionTree", "Linha 1", "Linha 2"));
    }

    @Override
    public void cleanup() {
    }
}
