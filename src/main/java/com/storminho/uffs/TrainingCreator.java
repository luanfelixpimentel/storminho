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
    int paresPositivos, paresTotais;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        try {
            ps = new PrintStream(Variables.arffPath + Variables.trainingOutputFile);
        } catch (Exception e) {
            System.out.println(e);
        }
        ss = Variables.trainingSampleSize;
        paresPositivos = paresTotais = 0;
        initializeArrfFile();
    }

    @Override
    public void execute(Tuple tuple) {
        Random random = new Random();
        int matchingInstances = (int)(ss * Variables.duplicatesTotal);
        double nonMatchRatio = (double)matchingInstances / Variables.totalPairs;

        System.out.println(tuple.getString(0));
        paresTotais++;
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

    //Write "frame" for the .arff file
    private void initializeArrfFile() {
        int qtdMethods = 0;

        //get the quantity of methods that gonna be used
        for (int aux = Variables.rankingMethods; aux != 0; aux = aux >> 1) {
            if ((1 & aux) != 0) qtdMethods++;
        }

        ps.print("@relation trainingSet\n");
        int columns = (Variables.attributesNumber - (Variables.fieldId + 1)) * qtdMethods;
        for (int i = 0; i < columns; i++) {
            ps.print("@attribute att" + i + " numeric\n");
        }
        ps.print("@attribute isDuplicate numeric\n@data\n");
        ps.flush();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }

    @Override
    public void cleanup() {
        ps.close();
        System.out.println("\n\n\nPARES DUPLICADOS: " + paresPositivos + " PARES TOTAIS: " + paresTotais + "\n\n\n");
    }
}
