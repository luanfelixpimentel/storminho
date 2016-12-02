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
import java.util.TreeSet;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import weka.core.DenseInstance;
import weka.core.Instances;

public class TrainingCreator extends BaseRichBolt implements IRichBolt {
    PrintStream ps;
    double ss;
    int paresPositivos, paresTotais, trDup, trNao;
    Instances dataRaw;
    private WekaStorminho ws;
    TreeSet<String> set;


    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        try {
            ps = new PrintStream(Variables.arffPath + Variables.trainingOutputFile);
        } catch (Exception e) {
            System.out.println(e);
        }
        set = null;
        set = new TreeSet<String>();
        paresPositivos = paresTotais = trDup = trNao = 0;

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
        String id1 = tuple.getString(2), id2 = tuple.getString(3);

        if (set.add(id1 + "_" + id2) && set.add(id2 + "_" + id1) && id1.equals(id2)) { //só vai passar por esse if aqueles que não foram considerados ainda e aqueles que não são exatamente igual (as redundâncias)
            paresTotais++;
            if (SharedMethods.isDuplicata(id1, id2)) { //Vai contar quantos pares distintos são duplicatas
                paresPositivos++;
                System.out.println(id1 + "\n" + id2);//////////////////////////////////////////////////////////////////////////////////////////
                if (random.nextDouble() < Variables.trainingSampleSize) { //ss = sample size
                    trDup++; //Quantas duplicatas entraram no set de treinamento
                } else {
                    return;
                }
            } else if (nonMatchRatio <= random.nextDouble()) {
                return;
            } else {
                trNao++; //Quantas não-duplicatas entraram no set de treinamento
            }
            ins.setDataset(dataRaw);
            ins.setClassValue((tuple.getInteger(1) == 1 ? "duplicata":"não-duplicata"));
            dataRaw.add(ins);
        }
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
        System.out.println("\n\n\nENTRARAM NO IF:\nPARES DUPLICATAS: " + paresPositivos + " PARES TOTAIS: " + paresTotais);
        System.out.println();
        System.out.println("ENTRARAM NO TREINAMENTO:\nPositivos: " + trDup + " e Negativos: " + trNao + "\n\n");
        System.out.println("\n");
    }
}
