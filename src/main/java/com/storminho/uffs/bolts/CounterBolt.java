package com.storminho.uffs.bolts;

import com.storminho.uffs.SharedMethods;
import com.storminho.uffs.Variables;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;

import java.util.Map;
import java.util.TreeSet;
import org.apache.storm.task.TopologyContext;

public class CounterBolt extends BaseRichBolt implements IRichBolt {
    OutputCollector _collector;
    long fp, fn, vp, vn;
    TreeSet<String> set;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        fp = fn = vp = vn = 0;
        set = new TreeSet<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String linha1 = tuple.getString(1), linha2 = tuple.getString(2); //Linhas do arquivo csv
        boolean respostaArvore = tuple.getInteger(0) == 1; //Resposta que a árvore do Weka deu pra semelhança calculada entre esse par de linhas
        String id1 = linha1.split(Variables.splitChars)[Variables.fieldId], id2 = linha2.split(Variables.splitChars)[Variables.fieldId]; //IDs das linhas (rec-XX-org/dup)

        //Nesse if só vai entrar o par que ainda não foi processado e que não seja a mesma linha
        if (set.add(id1 + "_" + id2) && set.add(id2 + "_" + id1) && !id1.equals(id2)) {
            if (SharedMethods.isDuplicata(id1, id2)) {
                if (respostaArvore) vp++;
                else fn++;
            } else {
                if (respostaArvore) fp++;
                else vn++;
            }
            // System.out.println("[c] " + respostaArvore);
            System.out.println("Falsos Positivos: " + fp + " Falsos Negativos: " + fn);
            System.out.println("Verdadeiros Positivos: " + vp + " Verdadeiros Negativos: " + vn);
            System.out.println("Precisão: " + 1.0 * vp / (vp + fp) + " Revocação: " + 1.0 * vp / (vp + fn));
            System.out.println();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
    }
}
