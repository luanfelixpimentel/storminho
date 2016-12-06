/*
TrainingCreatorBolt
Entrada: Uma Instance da biblioteca weka e 2 linhas originais do arquivo .csv
Saída: Nada
A Instance é a similaridade calculada entre as 2 linhas originais.
O Bolt cria um set de treinamento com uma porcentagem de pares de linhas que são passados para ele.
Esse treinamento é utilizado depois no DecisionTreeBolt.
O treinamento é um arquivo .arff que é salva a cada vez que uma nova Instance é inserida.
*/

package com.storminho.uffs.bolts;

import com.storminho.uffs.SharedMethods;
import com.storminho.uffs.Variables;
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

public class TrainingCreatorBolt extends BaseRichBolt implements IRichBolt {
    private PrintStream file;
    private int positiveTrainingPairs, negativeTrainingPairs, matchingInstances, allPairs, positivePairs;
    private Instances dataRaw;
    private TreeSet<String> set;
    private double nonMatchRatio, sampleSize;
    private Random random;


    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        set = new TreeSet<String>();
        sampleSize = Variables.SAMPLE_SIZE;
        matchingInstances = (int)(Variables.SAMPLE_SIZE * Variables.TOTAL_DUPLICATAS);
        nonMatchRatio = matchingInstances / Variables.TOTAL_PARES;
        random = new Random();
        positiveTrainingPairs = negativeTrainingPairs = allPairs = positivePairs = 0;

        //weka
        dataRaw = SharedMethods.newInstances("TrainingInstances");
    }

    @Override
    public void execute(Tuple tuple) {
        String linha1 = tuple.getString(1), linha2 = tuple.getString(2);
        DenseInstance instance = (DenseInstance)tuple.getValues().get(0);
        String id1 = linha1.split(Variables.SPLIT_CHARS)[Variables.FIELD_ID];
        String id2 = linha2.split(Variables.SPLIT_CHARS)[Variables.FIELD_ID];

        //só vai passar por esse if aqueles que não foram considerados ainda e aqueles que não são exatamente igual (as redundâncias)
        if (set.add(id1 + "_" + id2) && set.add(id2 + "_" + id1) && !id1.equals(id2)) {
            allPairs++;
            if (SharedMethods.isDuplicata(id1, id2)) { //Vai contar quantos desses pares distintos são duplicatas
                positivePairs++;
                if (random.nextDouble() < sampleSize) { //ss = sample size
                    positiveTrainingPairs++; //Quantas duplicatas entraram no set de treinamento
                } else {
                    return;
                }
            } else if (nonMatchRatio <= random.nextDouble() ) {
                return;
            } else {
                negativeTrainingPairs++; //Quantas não-duplicatas entraram no set de treinamento
            }

            //salva no arff
            instance.setDataset(dataRaw);
            instance.setClassValue((SharedMethods.isDuplicata(id1, id2) ? "duplicata":"não-duplicata"));
            dataRaw.add(instance);
            try {
                file = new PrintStream(Variables.ARFF_PATH + Variables.TRAININGSET_OUTPUT_FILE);
                file.print(dataRaw);
                file.flush();
                file.close();
            } catch (Exception e) { System.out.println(e); }

            //before run
            System.out.println("[tc] Total de pares: " + allPairs + " Pares positivos: " + positivePairs);

            //inform in console
            // System.out.println("ENTRARAM NO TREINAMENTO:\nPositivos: " + positiveTrainingPairs + " e Negativos: " + negativeTrainingPairs + "\n");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }

    @Override
    public void cleanup() {
    }
}
