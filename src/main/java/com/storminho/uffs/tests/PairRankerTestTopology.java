package com.storminho.uffs.tests;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.storminho.uffs.tests.PairRankerTestSpout;
import com.storminho.uffs.PairRanker;
import com.storminho.uffs.PairValidator;

public class PairRankerTestTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("test-spout", new PairRankerTestSpout(), 1);
    builder.setBolt("pair-ranker", new PairRanker(), 1).shuffleGrouping("test-spout");


    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(1);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test-topology", conf, builder.createTopology());
      System.out.println("\n\n\n=================================================");
      System.out.println("Não há mais linhas. Entrando em modo sleep agora.");
      System.out.println("=================================================\n\n\n");
      Thread.sleep(10000);
      cluster.shutdown();
    }
  }
}
