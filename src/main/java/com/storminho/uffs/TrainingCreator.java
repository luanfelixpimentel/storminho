/**Bolt criado apenas pra evitar que o PairRanker tenha que lidar com arquivos */

package com.storminho.uffs;

import java.io.PrintStream;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;

import java.util.Map;
import java.util.Random;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;

public class TrainingCreator extends BaseRichBolt implements IRichBolt {
    PrintStream ps;
    double ss;
    int paresPositivos;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        try {
            ps = new PrintStream(Variables.csvPath + Variables.trainingOutputFile);
        } catch (Exception e) {
            System.out.println(e);
        }
        ss = Variables.trainingSampleSize;
        paresPositivos = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        Random random = new Random();
        int matchingInstances = (int)(ss * Variables.duplicatesTotal);
        double nonMatchRatio = (double)matchingInstances / Variables.totalPairs;

        if (tuple.getInteger(1).equals(1)) {
            if (random.nextDouble() < ss) { //ss = sample size
                paresPositivos++;
            } else {
                return;
            }
        } else if (nonMatchRatio <= random.nextDouble()) {
            return;
        }
        ps.println(tuple.getString(0) + "," + tuple.getInteger(1));
        ps.flush();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }
    
    @Override
    public void cleanup() {
        ps.close();
    }
}
