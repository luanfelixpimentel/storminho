package edu.uffs.storminho;

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

public class LineSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private BufferedReader reader;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    try {
      reader = new BufferedReader(new FileReader(Variables.CSV_PATH + Variables.DATASET_INPUT));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

    @Override
    public void nextTuple() {
        try {
            String line = reader.readLine();
            if (line != null) {
                this._collector.emit(new Values(line));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("Linha"));
  }

  @Override
  public void deactivate() {
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
