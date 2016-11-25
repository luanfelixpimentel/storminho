package com.storminho.uffs;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class GuilhermeTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("line-spout", new LineSpout(), 5);
    builder.setBolt("line-save", new LineSaver(), 1).shuffleGrouping("line-spout");
    builder.setBolt("split-sentence", new SplitSentence(), 8).shuffleGrouping("line-spout");
    builder.setBolt("index-save", new WordIndexSave(), 1).shuffleGrouping("split-sentence");
 //   builder.setBolt("pair-generator", new PairGenerator(), 2).shuffleGrouping("index-save");
    builder.setBolt("pair-ranker", new PairRanker(), 2).shuffleGrouping("pair-generator");
    builder.setBolt("arvore", new Arvore(), 1).shuffleGrouping("pair-ranker");

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
      System.out.println("\n\n\n=================================================");
      System.out.println("Não há mais linha. Entrando em modo sleep agora.");
      System.out.println("=================================================\n\n\n");
      Thread.sleep(10000);
      cluster.shutdown();
    }
  }
}
