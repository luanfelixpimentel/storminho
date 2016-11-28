package com.storminho.uffs;


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

//simetria

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class PairRanker extends BaseRichBolt implements IRichBolt {
    private StringMetric cosineSim, jaccardSim, jaroWinklerSim, levenshteinSim, qGramsDistanceSim;
    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector c) {
        cosineSim = StringMetrics.cosineSimilarity();
        jaccardSim = StringMetrics.jaccard();
        jaroWinklerSim = StringMetrics.jaroWinkler();
        levenshteinSim = StringMetrics.levenshtein();
        qGramsDistanceSim = StringMetrics.qGramsDistance();
        _collector = c;
    }

    @Override
    public void execute(Tuple tuple) {
        boolean duplicata = isDuplicata(tuple.getString(0), tuple.getString(1)); //checa se são duplicatas
        String tuple1[] = tuple.getString(0).split(Variables.splitChars);
        String tuple2[] = tuple.getString(1).split(Variables.splitChars);
        String store = "";


        for (int i = Variables.fieldId + 1; i < Variables.attributesNumber; i++) {
            boolean last = i == Variables.attributesNumber - 1;
            if ((1 & Variables.rankingMethods) != 0) store += cosineSim.compare(tuple1[i], tuple2[i]) + ",";
            if ((2 & Variables.rankingMethods) != 0) store += jaccardSim.compare(tuple1[i], tuple2[i]) + ",";
            if ((4 & Variables.rankingMethods) != 0) store += jaroWinklerSim.compare(tuple1[i], tuple2[i]) + ",";
            if ((8 & Variables.rankingMethods) != 0) store += levenshteinSim.compare(tuple1[i], tuple2[i]) + ",";
            if ((16 & Variables.rankingMethods) != 0) store += qGramsDistanceSim.compare(tuple1[i], tuple2[i]) + ",";
        }

        // System.out.println(tuple2[1] + " # " + tuple1[1] + "\n" + store + " # " + duplicata); //just to see how things are going in terminal
        _collector.emit(new Values(store + (duplicata ? 1:0), (duplicata ? 1:0)));
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
