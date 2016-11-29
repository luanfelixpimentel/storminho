package com.storminho.uffs;


import java.util.ArrayList;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;

import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.StringMetrics;
import org.apache.storm.tuple.Values;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

//simetria

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class PairRanker extends BaseRichBolt implements IRichBolt {
    private StringMetric cosineSim, jaccardSim, jaroWinklerSim, levenshteinSim, qGramsDistanceSim;
    private OutputCollector _collector;
    private Instances dataRaw;
    private int instancesFieldsNumber;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector c) {
        cosineSim = StringMetrics.cosineSimilarity();
        jaccardSim = StringMetrics.jaccard();
        jaroWinklerSim = StringMetrics.jaroWinkler();
        levenshteinSim = StringMetrics.levenshtein();
        qGramsDistanceSim = StringMetrics.qGramsDistance();
        _collector = c;

        //weka
        instancesFieldsNumber = (Variables.attributesNumber - (Variables.fieldId + 1)) * countSim();
        ArrayList<Attribute> atts = new ArrayList<Attribute>(instancesFieldsNumber + 1);

        ArrayList<String> classVal = new ArrayList<String>(2);
        classVal.add("1");
        classVal.add("0");
        for (int i = 0; i < instancesFieldsNumber; i++) {
            atts.add(new Attribute("att" + i));
        }
        atts.add(new Attribute("resultado", classVal));
        dataRaw = new Instances("TestInstances",atts,0);
        dataRaw.setClassIndex(dataRaw.numAttributes() - 1);
        // dataRaw.setClass(new Attribute("resultado", classVal));
    }

    //count the true bits to count how many methods will be used
    static private int countSim() {
        int rm = Variables.rankingMethods, count = 0;
        for (; rm != 0; rm /= 2) {
            if ((1 & rm) != 0) count++;
        }
        return count;
    }

    @Override
    public void execute(Tuple tuple) {
        boolean duplicata = isDuplicata(tuple.getString(0), tuple.getString(1)); //checa se são duplicatas
        String tuple1[] = tuple.getString(0).split(Variables.splitChars);
        String tuple2[] = tuple.getString(1).split(Variables.splitChars);
        String store = "";
        double[] instanceValues = new double[instancesFieldsNumber];

        //for for instance
        for (int i = 0, j = Variables.fieldId + 1; i < instancesFieldsNumber; j++) {
            if ((1 & Variables.rankingMethods) != 0) instanceValues[i++] = cosineSim.compare(tuple1[j], tuple2[j]);
            if ((2 & Variables.rankingMethods) != 0) instanceValues[i++] = jaccardSim.compare(tuple1[j], tuple2[j]);
            if ((4 & Variables.rankingMethods) != 0) instanceValues[i++] = jaroWinklerSim.compare(tuple1[j], tuple2[j]);
            if ((8 & Variables.rankingMethods) != 0) instanceValues[i++] = levenshteinSim.compare(tuple1[j], tuple2[j]);
            if ((16 & Variables.rankingMethods) != 0) instanceValues[i++] = qGramsDistanceSim.compare(tuple1[j], tuple2[j]);
        }

        Instance inst = new DenseInstance(1.0, instanceValues);
        inst.setDataset(dataRaw);
        dataRaw.add(0, inst);
        System.out.println("First: " + dataRaw.firstInstance());
        System.out.println("Last: " + dataRaw.lastInstance());
        System.out.println("Number of Instances: " + dataRaw.numInstances());
        System.out.println();
        _collector.emit(new Values(new Instances(dataRaw), (duplicata ? 1:0)));
        dataRaw.remove(0);
    }

    //this method only checks if two tuples are duplicatas according to the number in the first column
    private static boolean isDuplicata(String tupleA, String tupleB) {
        //split the tuples' indexes to separe the identifier
        String[] aSplit = tupleA.split(Variables.indexSplitToken);
        String[] bSplit = tupleB.split(Variables.indexSplitToken);

        //check if the identifier of both are equal
        return (Integer.parseInt(aSplit[1]) == Integer.parseInt(bSplit[1]));
    }

    @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("similaridade", "resposta_certa"));
    }

    @Override
    public void cleanup() {
    }
}
