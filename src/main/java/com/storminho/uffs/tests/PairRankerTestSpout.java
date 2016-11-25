package com.storminho.uffs.tests;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.topology.IRichSpout;
import com.storminho.uffs.Variables;

public class PairRankerTestSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private BufferedReader reader;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    try {
      reader = new BufferedReader(new FileReader(Variables.csvPath + "cd-100.csv"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void nextTuple() {
      try {
          String line = reader.readLine();
          String line2 = reader.readLine();
          if (line2 != null) {
              this._collector.emit(new Values(line, line2));
          } else {
              Thread.sleep(1000);
          }
      } catch (Exception e) {
          e.printStackTrace();
      }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("line", "line2"));
  }

  @Override
  public void deactivate() {
      System.out.println("\n\nDeactivating the topology\n\n");
      try {
          reader.close();
      } catch (IOException e) {
          System.out.println(e);
      }
  }


  @Override
  public void fail(Object id) { System.err.println("Failed line number " + id); }

  @Override
  public void close() {}

  public boolean isDistributed() { return false; }

  @Override
  public void activate() {}

  @Override
  public void ack(Object msgId) {}

  @Override
  public Map<String, Object> getComponentConfiguration() { return null; }


}
