package com.storminho.uffs;


import java.io.PrintStream;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;

import java.util.Map;
import org.apache.storm.task.TopologyContext;

public class Counter extends BaseRichBolt implements IRichBolt {
    OutputCollector _collector;
    long fp, fn, vp, vn;
    PrintStream ps;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        try {
            ps = new PrintStream(Variables.outPath + Variables.counterOutputFile);
        } catch (Exception e) {
            System.out.println(e);
        }
        fp = fn = vp = vn = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getInteger(0).equals(tuple.getInteger(1))) {
            if (tuple.getInteger(0).equals(1)) vp++;
            else vn++;
        }
        else {
            if (tuple.getInteger(0).equals(1)) fp++;
            else fn++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup() {
        ps.println("Falsos Positivos: " + fp);
        ps.println("Falsos Negativos: " + fn);
        ps.println("Verdadeiros Positivos: " + vp);
        ps.println("Verdadeiros Negativos: " + vn);
        ps.println("Precisão: " + (double)vp / (double)(vp + fp));
        ps.println("Revocação: " + (double)vp / (double)(vp + fn));
        ps.flush();
        ps.close();
    }
}
