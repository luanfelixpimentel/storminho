package com.storminho.uffs;

import com.storminho.uffs.bolts.CounterBolt;
import com.storminho.uffs.bolts.DecisionTreeBolt;
import com.storminho.uffs.bolts.PairRankerBolt;
import com.storminho.uffs.bolts.SplitSentenceBolt;
import com.storminho.uffs.bolts.TrainingCreatorBolt;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class GuilhermeTopology {

  public static void main(String[] args) throws Exception {
    
    TopologyBuilder builder = new TopologyBuilder();

//    JedisPoolConfig poolConfig = new JedisPoolConfig();
//    poolConfig.setTestOnBorrow(true);
//    poolConfig.setTestOnReturn(true);
//    poolConfig.setMaxTotal(1);
//    poolConfig.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(60));
//    JedisPool pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1");


    builder.setSpout("line-spout", new LineSpout());
    builder.setBolt("line-saver", new LineSaver(), 5).shuffleGrouping("line-spout");
    builder.setBolt("split-sentence", new SplitSentenceBolt(), 5).shuffleGrouping("line-spout");
    builder.setBolt("index-save", new WordIndexSave(), 5).shuffleGrouping("split-sentence");
    builder.setBolt("pair-generator", new PairGenerator(), 10).shuffleGrouping("index-save");
    builder.setBolt("pair-ranker", new PairRankerBolt(), 5).shuffleGrouping("pair-generator");
//    builder.setBolt("training-creator", new TrainingCreatorBolt(), 1).shuffleGrouping("pair-ranker");
    builder.setBolt("decisiontree", new DecisionTreeBolt(), 1).shuffleGrouping("pair-ranker");
    builder.setBolt("counter", new CounterBolt(), 1).shuffleGrouping("decisiontree");

    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(1);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(1);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count-topology", conf, builder.createTopology());
      System.out.println("\n\n\n=================================================");
      System.out.println("Topologia chegou ao fim de sua vida. Deixa pra trás 3 filhos. Rezemos.");
      System.out.println("=================================================\n\n\n");
    }
//    pool.close();
  }
}
