/*
CounterBolt
Entrada: duas linhas originais e a resposta dada pela árvore para a similaridade dessas duas linhas.
Saída: Nada
Enquanto a topologia roda, esse bolt imprimirá no terminal quanos Falsos/Verdadeiros Positivos/Negativos foram computados, a precisão e a revocação da árvore.
*/

package edu.uffs.storminho.bolts;

import edu.uffs.storminho.SharedMethods;
import edu.uffs.storminho.Variables;
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
        String id1 = linha1.split(Variables.SPLIT_CHARS)[Variables.FIELD_ID], id2 = linha2.split(Variables.SPLIT_CHARS)[Variables.FIELD_ID]; //IDs das linhas (rec-XX-org/dup)

         //Nesse if só vai entrar o par que ainda não foi processado e que não seja a mesma linha
        if(!(linha1.contains("dup") && linha2.contains("dup")))
        if (set.add(id1 + "_" + id2) && set.add(id2 + "_" + id1) && !id1.equals(id2)) {
            if (SharedMethods.isDuplicata(id1, id2)) {
                if (respostaArvore) { vp++; }
                else { fn++; }
            } else {
                if (respostaArvore) { fp++; }
                else { vn++; }
            }

            // System.out.println("[c] " + respostaArvore);
            if ((vp + vn + fp + fn) % 1000 == 0) {
                System.out.println("Falsos Positivos: " + fp + " Falsos Negativos: " + fn);
                System.out.println("Verdadeiros Positivos: " + vp + " Verdadeiros Negativos: " + vn);
                System.out.println("Precisão: " + 1.0 * vp / (vp + fp) + " Revocação: " + 1.0 * vp / (vp + fn));
                System.out.println((vp + vn + fp + fn) / 1000 + " mil pares computados.");
                System.out.println();
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
    }
}
