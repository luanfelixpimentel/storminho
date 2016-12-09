package edu.uffs.storminho.tests;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import edu.uffs.storminho.*;
import edu.uffs.storminho.bolts.LineSaverBolt;
import edu.uffs.storminho.bolts.PairGeneratorBolt;
import edu.uffs.storminho.bolts.SplitSentenceBolt;
import edu.uffs.storminho.bolts.WordIndexSaveBolt;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class RedisTestTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setMaxIdle(400);
    // Tests whether connections are dead during idle periods
    poolConfig.setTestWhileIdle(true);
    poolConfig.setMaxTotal(400);
    //configuring it for some good max value so that timeout don't occur
    poolConfig.setMaxWaitMillis(120000);
    JedisPool pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");

    builder.setSpout("line-spout", new LineSpout());
    builder.setBolt("line-saver", new LineSaverBolt(), 1).shuffleGrouping("line-spout");
    builder.setBolt("split-sentence", new SplitSentenceBolt(), 8).shuffleGrouping("line-spout");
    builder.setBolt("index-save", new WordIndexSaveBolt(), 1).shuffleGrouping("split-sentence");
    builder.setBolt("pair-generator", new PairGeneratorBolt(), 2).shuffleGrouping("index-save");

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
