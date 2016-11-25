/**Bolt criado apenas pra evitar que o PairRanker tenha que lidar com arquivos */

package com.storminho.uffs;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;

import java.util.Map;
import org.apache.storm.task.TopologyContext;

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
        ss = Variables.trainingSampleSize; //selecionar 5% dos pares positivos
        paresPositivos = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        Random random = new Random();

        //Como sei quantas duplicatas tenho que por aqui? Tenho que rodar uma vez pra contar?
        int matchingInstances = (int)(SAMPLE_SIZE*duplicates.size());

        //Como sei quantos pares eu uso aqui? Tenho que também rodar uma vez pra contar?
        double nonMatchRatio = matchingInstances / (totalDePares);

        //Aqui tá igual o código que você mandou. Só adaptei alguns nomes e tal.
        if (tuple.getInt(1) == 1) {
            if (random.nextDouble() < ss) { //ss = sample size
                paresPositivos++;
            } else {
                continue;
            }
        } else if (nonMatchRatio <= random.nextDouble()) {
            continue;
        }

        //Aqui eu tô salvando só os pares positivos que passaram no "ss" e os negativos que passaram no "nonMatchRatio".
        //É isso mesmo que tem que fazer?
        ps.println(tuple.getString(0) + "," + tuple.getInt());
        ps.flush();
    }

    @Override
    public void cleanup() {
        ps.close();
    }
}
