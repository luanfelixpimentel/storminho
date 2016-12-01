package com.storminho.uffs.tests;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import com.storminho.uffs.*;

public class RedisTestTopology {

  public static void main(String[] args) throws Exception {
      
    TopologyBuilder builder = new TopologyBuilder();
    JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();
    
    builder.setSpout("line-spout", new LineSpout());
    builder.setBolt("line-saver", new LineSaver(), 1).shuffleGrouping("line-spout");
 //   builder.setBolt("pair-generator", new PairGenerator(), 1).shuffleGrouping("line-saver");

    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
    //   conf.setMaxTaskParallelism(1);
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
