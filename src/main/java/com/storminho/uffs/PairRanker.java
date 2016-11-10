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

//simetria

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class PairRanker extends BaseBasicBolt {
    PrintStream ps;

    @Override
    public void prepare(Map map, TopologyContext context) {
        try {
            ps = new PrintStream(GlobalVariables.arffPath);
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String tuple1[] = tuple.getString(0).split(GlobalVariables.splitChars);
        String tuple2[] = tuple.getString(1).split(GlobalVariables.splitChars);
        System.out.println(tuple2[0] + tuple1[0] + "\n\n");

        //compiling teste
        System.out.println("\n\n" + tuple1[GlobalVariables.fieldId] + tuple2[GlobalVariables.fieldId] + "\n\n");

        boolean checado = checaDuplicata(tuple1[GlobalVariables.fieldId], tuple2[GlobalVariables.fieldId]);

        String store = "";

        //Methods
//        TokeniserQGram2 tok = new TokeniserQGram2();
//		JaccardSimilarity jc = new JaccardSimilarity(tok);
//		QGramsDistance ngram = new QGramsDistance(tok);
//		JaroWinkler jw = new JaroWinkler();
//		Levenshtein le = new Levenshtein();
//		CosineSimilarity cos = new CosineSimilarity(tok);

        //zero values
        Float Vgram = null, Vle = null, Vjar = null, Vcos = null, Vjw = null;

        // for (int i = GlobalVariables.fieldId + 1; i < GlobalVariables.attributesNumber; i++) {
        //     String att1 = tuple1[i];
        //     String att2 = tuple2[i];

        // //get the similarities values
        //    Vgram = ngram.getSimilarity(att1, att2);
        //    Vle = le.getSimilarity(att1, att2);
        //    Vjar = jc.getSimilarity(att1, att2);
        //    Vcos = cos.getSimilarity(att1, att2);
        //    Vjw = jw.getSimilarity(att1, att2);
        //
        //     if(Vgram!=Float.NaN && !Vgram.isNaN() && Vgram!=null) {
        //         store=store+Vgram.toString()+", ";
        //     } else {
        //         store=store+0F+", ";
        //     }
        //     if(GlobalVariables.rankingMethods >= 2 && Vle!=Float.NaN && !Vle.isNaN() && Vle!=null) {
        //         store=store+Vle.toString()+", ";
        //     } else {
        //         store=store+0F+", ";
        //     }
        //     if(GlobalVariables.rankingMethods >= 3 && Vjar!=Float.NaN && !Vjar.isNaN() && Vjar!=null) {
        //         store=store+Vjar.toString()+", ";
        //     } else {
        //         store=store+0F+", ";
        //     }
        //     if(GlobalVariables.rankingMethods >=  4 && Vcos!=Float.NaN && !Vcos.isNaN() && Vcos!=null) {
        //         store=store+Vcos.toString()+", ";
        //     } else {
        //         store=store+0F+", ";
        //     }
        // }
        // ps.print(store);
        // ps.println(checado ? 1:0);
        // ps.flush();
    }

    private boolean checaDuplicata(String a, String b) {
        String aa[] = a.split(GlobalVariables.indexSplitToken);
        String bb[] = b.split(GlobalVariables.indexSplitToken);

        //access the number of the tuple
        return aa[2] == bb[2];
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
