//antigo PairValidator que foi transformado em código normal, ao contrário do que era antes, um bolt
package com.storminho.uffs;

import java.util.HashSet;

public class CorrectnessCounter {
    private HashSet set;
    private int errorCounter, dupCounter;

    public CorrectnessCounter() {
        this.errorCounter = dupCounter = 0;
        this.set = new HashSet();
    }

    //this method only checks if two tuples are duplicatas
    public static boolean isDuplicata(String tupleA, String tupleB) {
        //split the tuples' indexes to separe the identifier
        String[] aSplit = tupleA.split(GlobalVariables.indexSplitToken);
        String[] bSplit = tupleB.split(GlobalVariables.indexSplitToken);

        //check if the identifier of both are equal
        return (Integer.parseInt(aSplit[1]) == Integer.parseInt(bSplit[1]));
    }

    //this method will check duplicatas and increase the counter if the pair haven't been proccessed yet
	public void countDuplicatas(String tupleA, String tupleB) {
        //flag to check if the pair was already processed
        boolean flag = true;

        //split the tuples' indexes to separe the identifier
        String[] aSplit = tupleA.split(GlobalVariables.indexSplitToken);
        String[] bSplit = tupleB.split(GlobalVariables.indexSplitToken);

        flag &= this.set.add(aSplit[GlobalVariables.fieldId] + "#" + bSplit[GlobalVariables.fieldId]);
        flag &= this.set.add(bSplit[GlobalVariables.fieldId] + "#" + aSplit[GlobalVariables.fieldId]);
        if (flag) {
            if (isDuplicata(tupleA, tupleB)) {
                dupCounter++;
            } else {
                errorCounter++;
            }
        }
    }

    @Override
    public String toString() {
        return "Duplications found: " + dupCounter + ". Errors found: " + errorCounter;
    }

}
