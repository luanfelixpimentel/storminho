package com.storminho.uffs;
import java.text.BreakIterator;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.storminho.uffs.Variables;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class SplitSentence extends BaseBasicBolt {

  //Execute is called to process tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String[] allWords = tuple.getString(0).split(Variables.splitChars);
    int idField = Variables.fieldId;
    //Send every word from the tuple to collector, except the id
    for (int i = idField + 1; i < allWords.length; i++) { //print all to test
        collector.emit(new Values(allWords[i], allWords[idField]));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "id"));
  }
}
