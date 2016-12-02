/**
* Methods that can be used by more than one bolt
*/

package com.storminho.uffs;

import java.io.PrintStream;

public class SharedMethods {
    /**
    *This method only checks if two tuples are duplicatas according to the number in the first column.
    *@param idA the id field of a tuple
    *@param idB the id field of a tuple
    */
    public static boolean isDuplicata(String idA, String idB) {
        //split the tuples' indexes to separe the identifier
        String[] aSplit = idA.split(Variables.indexSplitToken);
        String[] bSplit = idB.split(Variables.indexSplitToken);

        //check if the identifier of both are equal
        boolean flag = false;
        try {
            flag = (Integer.parseInt(aSplit[1]) == Integer.parseInt(bSplit[1]));
        } catch (Exception e) {
            System.out.println("com.storminho.uffs.PairRanker.isDuplicata()");
        }
            return flag;
    }

    /**
    * Write the basic header for a .arff
    */
    public static void initializeArrfFile(PrintStream ps) {
        int qtdMethods = 0;

        //get the quantity of methods that gonna be used
        for (int aux = Variables.rankingMethods; aux != 0; aux = aux >> 1) {
            if ((1 & aux) != 0) qtdMethods++;
        }

        ps.print("@relation trainingSet\n");
        int columns = (Variables.attributesNumber - (Variables.fieldId + 1)) * qtdMethods;
        for (int i = 0; i < columns; i++) {
            ps.print("@attribute att" + i + " numeric\n");
        }
        ps.print("@attribute isDuplicate numeric\n@data\n");
        ps.flush();
    }
}
