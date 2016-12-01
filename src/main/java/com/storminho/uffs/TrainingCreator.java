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
import weka.core.DenseInstance;
import weka.core.Instances;

public class TrainingCreator extends BaseRichBolt implements IRichBolt {
    PrintStream ps;
    double ss;
    int paresPositivos, paresTotais;
    Instances dataRaw;
    private WekaStorminho ws;


    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        try {
            ps = new PrintStream(Variables.arffPath + Variables.trainingOutputFile);
        } catch (Exception e) {
            System.out.println(e);
        }
        ss = Variables.trainingSampleSize;
        paresPositivos = paresTotais = 0;

        //weka
        WekaStorminho ws = new WekaStorminho();
        dataRaw = ws.newInstances("TrainingInstances");

    }

    @Override
    public void execute(Tuple tuple) {
        DenseInstance ins = (DenseInstance)tuple.getValues().get(0);
        Random random = new Random();
        int matchingInstances = (int)(ss * Variables.duplicatesTotal);
        double nonMatchRatio = (double)matchingInstances / Variables.totalPairs;

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
        ins.setDataset(dataRaw);
        ins.setClassValue((tuple.getInteger(1) == 1 ? "duplicata":"não-duplicata"));
        dataRaw.add(ins);
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
        ps.print(dataRaw);
        ps.flush();
        ps.close();
        System.out.println("\n\n\nPARES DUPLICADOS: " + paresPositivos + " PARES TOTAIS: " + paresTotais + "\n\n\n");
    }
}
