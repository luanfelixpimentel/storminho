/*
PairRankerBolt
Entrada: Linhas para serem avaliadas. Aplicará os métodos definidos na constante Variables.RANKING_METHODS.
Saída: Similaridade das linhas recebidas e as duas linhas que recebeu como entrada.
A similaridade é calculada nos campos das linhas. Cada campo terá sua similaridade calculada em todos os métodos ativos antes de ser calculado o próximo campo.
Ex:
Linha 1 = campo1:campo2:...
Linha 2 = campoa:campob:...
Métodos que serão utilizados: Jaccard e Leveshtein
Saída: Jaccard(campo1, campoa), Levenshtein(campo1, campoa), Jaccard(campo2, campob), Levenshtein(campo2, campob), ..., classe
A classe será a resposta certa se é ou não uma duplicata (Nos casos em que essa resposta esteja explícita através do campo ID das linhas) ou "?" caso contrário.
*/
package edu.uffs.storminho.bolts;

import edu.uffs.storminho.*;
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
import weka.core.DenseInstance;

//simetria

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class PairRankerBolt extends BaseRichBolt implements IRichBolt {
    private StringMetric cosineSim, jaccardSim, jaroWinklerSim, levenshteinSim, qGramsDistanceSim;
    private OutputCollector _collector;
    private boolean countMode;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector c) {
        cosineSim = StringMetrics.cosineSimilarity();
        jaccardSim = StringMetrics.jaccard();
        jaroWinklerSim = StringMetrics.jaroWinkler();
        levenshteinSim = StringMetrics.levenshtein();
        qGramsDistanceSim = StringMetrics.qGramsDistance();
        _collector = c;
        countMode = Variables.COUNT_MODE;
    }

    @Override
    public void execute(Tuple tuple) {
        String linha1 = tuple.getString(0), linha2 = tuple.getString(1);
        String tuple1[] = linha1.split(Variables.SPLIT_CHARS);
        String tuple2[] = linha2.split(Variables.SPLIT_CHARS);
        double[] instanceValues = new double[Variables.getFieldsNumber() + 1];
        String cmp1, cmp2;

        if (!countMode) {
            //for for instance
            for (int i = 0, j = Variables.FIELD_ID + 1; j < Variables.ATTRIBUTES_NUMBER; j++) {
                cmp1 = tuple1[j].trim();
                cmp2 = tuple2[j].trim();
                if ((1 & Variables.RANKING_METHODS) != 0) instanceValues[i++] = cosineSim.compare(cmp1, cmp2);
                if ((2 & Variables.RANKING_METHODS) != 0) instanceValues[i++] = jaccardSim.compare(cmp1, cmp2);
                if ((4 & Variables.RANKING_METHODS) != 0) instanceValues[i++] = jaroWinklerSim.compare(cmp1, cmp2);
                if ((8 & Variables.RANKING_METHODS) != 0) instanceValues[i++] = levenshteinSim.compare(cmp1, cmp2);
                if ((16 & Variables.RANKING_METHODS) != 0) instanceValues[i++] = qGramsDistanceSim.compare(cmp1, cmp2);
            }
        }
        DenseInstance instance = new DenseInstance(1.0, instanceValues);


        //out test
        //System.out.println("[pr]" + instance + "\n" + linha1 + "\n" + linha2 + "\n");

        _collector.emit(new Values(instance, linha1, linha2));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("weka:Instance", "linha1", "linha2"));
    }

    @Override
    public void cleanup() {}
}
