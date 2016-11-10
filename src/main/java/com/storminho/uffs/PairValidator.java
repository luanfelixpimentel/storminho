package com.storminho.uffs;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import com.storminho.uffs.GlobalVariables;
import java.util.TreeSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class PairValidator extends BaseBasicBolt {
    private static HashMap<Integer, Set> indexes = null;
    private static long errorCounter, dupCounter;

    @Override
    public void prepare(Map map, TopologyContext context) {
        if (indexes == null) {
            this.errorCounter = dupCounter = 0;
            indexes = new HashMap<Integer, Set>();
        }
    }


    private static boolean insertIntoIndexes(Integer p, Integer k) {
        Set ind = indexes.get(p);
        if (ind == null) {
            ind = new TreeSet();
            ind.add(k);
            indexes.put(p, ind);
        }
        else {
            return indexes.get(p).add(k);
        }
        return true;
    }

    @Override
	public void execute(Tuple input, BasicOutputCollector collector) {
        Integer keyA = -2, valueA = -2;
        boolean aHas = input.getString(0).contains(GlobalVariables.dupToken);
        String[] aSplit = input.getString(0).split(GlobalVariables.indexSplitToken);

        Integer keyB = -2, valueB = -2;
        boolean bHas = input.getString(1).contains(GlobalVariables.dupToken);
        String[] bSplit = input.getString(1).split(GlobalVariables.indexSplitToken);
        System.out.println("[" + aSplit[1] + " " + bSplit[1] + "]\n");
        //if none of them has the dup subtoken, they does not share the same value
        if (aHas || bHas) {
            if (aHas) {
                keyA = Integer.parseInt(aSplit[1]);
                valueA = Integer.parseInt(aSplit[3]);
            } else {
                keyA = Integer.parseInt(aSplit[1]);
            }

            if (bHas) {
                keyB = Integer.parseInt(bSplit[1]);
                valueB = Integer.parseInt(bSplit[3]);
            } else {
                keyB = Integer.parseInt(bSplit[1]);
            }

            if (keyA.equals(keyB)) {
                boolean flag = false;
                //if the methods call return true, that means the value we are inserting into hashmap did not exist there yet
                if (aHas) flag |= insertIntoIndexes(keyA, valueA);
                if (bHas) flag |= insertIntoIndexes(keyB, valueB);
                if (flag) dupCounter++;
            } else {
                errorCounter++;
            }
        } else {
            errorCounter++;
        }
    }

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        System.out.println("\n\n\nDuplications found: " + dupCounter + " . Errors found: " + errorCounter + "\n\n");
    }

}
