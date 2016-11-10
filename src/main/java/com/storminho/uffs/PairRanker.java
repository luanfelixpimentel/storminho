package com.storminho.uffs;

import java.text.BreakIterator;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.storminho.uffs.GlobalVariables;

import java.io.PrintStream;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import java.io.IOException;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.StringMetrics;

//simetria

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class PairRanker extends BaseBasicBolt {
    PrintStream ps;

    @Override
    public void prepare(Map map, TopologyContext context) {
        try {
            ps = new PrintStream(GlobalVariables.arffPath);
            initializeArrfFile();
        } catch (IOException e) {
            System.out.println(e);
        }
    }


    //write the simmetrics between two tuples in the .arff file
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String tuple1[] = tuple.getString(0).split(GlobalVariables.splitChars);
        String tuple2[] = tuple.getString(1).split(GlobalVariables.splitChars);
        System.out.println(tuple2[0] + tuple1[0] + "\n\n");

        boolean checado = checaDuplicata(tuple1[GlobalVariables.fieldId], tuple2[GlobalVariables.fieldId]);

        String store = "";

        //initialization
        StringMetric cosineSimilarity = StringMetrics.cosineSimilarity();
        StringMetric jaccardSimilarity = StringMetrics.jaccard();
        StringMetric jaroWinklerSimilarity = StringMetrics.jaroWinkler();
        StringMetric levenshteinSimilarity = StringMetrics.levenshtein();
        StringMetric qGramsDistanceSimilarity = StringMetrics.qGramsDistance();

        for (int i = GlobalVariables.fieldId + 1; i < GlobalVariables.attributesNumber; i++) {
            if ((1 & GlobalVariables.rankingMethods) != 0) store += cosineSimilarity.compare(tuple1[i], tuple2[i]) + ",";
            if ((2 & GlobalVariables.rankingMethods) != 0) store += jaccardSimilarity.compare(tuple1[i], tuple2[i]) + ",";
            if ((4 & GlobalVariables.rankingMethods) != 0) store += jaroWinklerSimilarity.compare(tuple1[i], tuple2[i]) + ",";
            if ((8 & GlobalVariables.rankingMethods) != 0) store += levenshteinSimilarity.compare(tuple1[i], tuple2[i]) + ",";
            if ((16 & GlobalVariables.rankingMethods) != 0) store += qGramsDistanceSimilarity.compare(tuple1[i], tuple2[i]) + ",";
        }
        ps.print(store);
        ps.println(checado ? 1:0);
        ps.flush();
    }

    //Write "frame" for the .arff file
    private void initializeArrfFile() {
        int qtdMethods = 0;

        //get the quantity of methods that gonna be used
        for (int aux = GlobalVariables.rankingMethods; aux != 0; aux = aux >> 1) {
            if ((1 & aux) != 0) qtdMethods++;
        }

        ps.print("@relation simetria-tuplas\n");
        int columns = (GlobalVariables.attributesNumber - (GlobalVariables.fieldId + 1)) * qtdMethods;
        for (int i = 0; i < columns; i++) {
            ps.print("@attribute att" + i + " numeric\n");
        }
        ps.print("@attribute isDuplicate numeric\n@data\n");
        ps.flush();
    }

    //check if two tuples share the same number id
    private boolean checaDuplicata(String a, String b) {
        String aa[] = a.split(GlobalVariables.indexSplitToken);
        String bb[] = b.split(GlobalVariables.indexSplitToken);

        //access the number of the tuple
        return aa[1].equals(bb[1]);
    }

    @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void cleanup() {
        ps.close();
    }
}
