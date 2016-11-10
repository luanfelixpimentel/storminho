package com.storminho.uffs;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.task.TopologyContext;

public class WordIndexSave extends BaseBasicBolt {
    Map<String, Set> indexes;

    @Override
    public void prepare(Map map, TopologyContext context) {
        this.indexes = new HashMap<String, Set>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      String lineId = tuple.getString(1);
      Set linesIndexes = this.indexes.get(word);

      //insert word and id into the set
      try {
          if (this.indexes.get(word) == null) {
              this.indexes.put(word, new TreeSet<String>());
              this.indexes.get(word).add(lineId);
          }
          else {
            this.indexes.get(word).add(lineId);
          }
      } catch(Exception e) {
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }

    @Override
    public void cleanup() {
        //print all the set before leave topology
        Iterator<Set> lt = this.indexes.values().iterator();
        Iterator<String> st = this.indexes.keySet().iterator();
        while (lt.hasNext()) {
            Iterator<String> it = lt.next().iterator();
            System.out.print("Palavra [" + st.next() + "]");
            while (it.hasNext()) {
                System.out.print(" " + it.next() +" ");
            }
            System.out.println();
        }
    }
  }
