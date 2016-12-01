package com.storminho.uffs;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class WordCountTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("line-spout", new LineSpout(), 5);
    builder.setBolt("split-sentence", new SplitSentence(), 8).shuffleGrouping("line-spout");
    // builder.setBolt("word-count", new WordCount(), 1).shuffleGrouping("split-sentence");
    builder.setBolt("index-save", new WordIndexSave(), 1).shuffleGrouping("split-sentence");


    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count-topology", conf, builder.createTopology());
    //   System.out.println("Acabaram-se as linhas. Desligando em alguns segundos.");
      Thread.sleep(10000);
      cluster.shutdown();
    }
  }
}
