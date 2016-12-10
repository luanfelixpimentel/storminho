/*
Entrada: Uma linha original do arquivo .csv
Saida: O id dessa linha e um campo da linha. Todos os campos serão emitidos.
Quebra a linha em algumas palavras e manda adiante, junto com o id, pra verificar a origem.
*/
package edu.uffs.storminho.bolts;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import edu.uffs.storminho.Variables;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class SplitSentenceBolt extends BaseRichBolt implements IRichBolt{
    OutputCollector _collector;
    private int idField;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
        String[] allWords = tuple.getString(0).split(Variables.SPLIT_CHARS);
        int idField = Variables.FIELD_ID;
        //Send every word from the tuple to collector, except the id
        for (int i = idField + 1; i < allWords.length; i++) { //print all to test
            if (!allWords[i].equals("")) { _collector.emit(new Values(allWords[i], allWords[idField])); }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Palavra", "ID"));
    }

    @Override
    public void cleanup() {}
}
