//antigo PairValidator que foi transformado em código normal, ao contrário do que era antes, um bolt
package com.storminho.uffs;

import com.storminho.uffs.GlobalVariables;
import java.util.TreeSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.HashSet;

public class CorrectnessCounter {
    private static HashMap<Integer, Set> indexes = null;
    private static long errorCounter, dupCounter;

    public void prepare(Map map, TopologyContext context) {
        if (indexes == null) {
            this.errorCounter = dupCounter = 0;
            indexes = new HashMap<Integer, Set>();
        }
    }

    //this method will insert both integers into the set's hashmap. If both of them already are in the map, the method will return false.
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

    //this method only checks if two tuples are duplicatas
    public boolean isDuplicata(String tupleA, String tupleB) {
        //split the tuples' indexes to separe the identifier
        String[] aSplit = tupleA.split(GlobalVariables.indexSplitToken);
        String[] bSplit = tupleB.split(GlobalVariables.indexSplitToken);

        //check if the identifier of both are equal
        return (Integer.parseInt(aSplit[1]).equals(Integer.parseInt(bSplit[1])));
    }

    //this method will check duplicatas and increase the counter if the pair haven't been proccessed yet
	public void countDuplicatas(String tupleA, String tupleB) {
        Integer keyA = -2, valueA = -2;
        String[] aSplit = tupleA.split(GlobalVariables.indexSplitToken);
        boolean aHas = aSplit[GlobalVariables.fieldId].contains(GlobalVariables.dupToken);

        Integer keyB = -2, valueB = -2;
        String[] bSplit = tupleB.split(GlobalVariables.indexSplitToken);
        boolean bHas = tupleB[GlobalVariables.fieldId].contains(GlobalVariables.dupToken);


        if (isDuplicata(tupleA, tupleB)) {
            if (aHas) { valueA = Integer.parseInt(aSplit[3]); }
            if (bHas) { valueB = Integer.parseInt(bSplit[3]); }
            boolean flag = false;
            flag |= insertIntoIndexes(keyA, valueA);
            flag |= insertIntoIndexes(keyB, valueB);
        }

        if (keyA.equals(keyB)) {
                //if the methods call return true, that means the value we are inserting into hashmap did not exist there yet (at least one of them)
                if (flag) dupCounter++;
            } else {
                errorCounter++;
            }
        } else {
            errorCounter++;
        }
    }

    public void printResults() {
        System.out.println("\n\n\nDuplications found: " + dupCounter + " . Errors found: " + errorCounter + "\n\n");
    }

}
