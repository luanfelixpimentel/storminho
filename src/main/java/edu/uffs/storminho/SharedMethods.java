/*
SharedMethods
Methods that can be used by more than one bolt
*/

package edu.uffs.storminho;

import java.io.PrintStream;
import java.util.ArrayList;
import weka.core.Attribute;
import weka.core.Instances;

public class SharedMethods {

    //receives a id like (rec-XXXX-org or rec-XXXX-dup-X) and tell if it is a duplicate
    public static boolean isDuplicata(String idA, String idB) {
        //split the tuples' indexes to separe the identifier
        String[] aSplit = idA.split(Variables.ID_SPLIT_CHARS);
        String[] bSplit = idB.split(Variables.ID_SPLIT_CHARS);
        return  aSplit[1].equals(bSplit[1]) && (aSplit[2].equals("org") ^ bSplit[2].equals("org"));
    }

    //Write the basic header for a .arff file
    public static void initializeArrfFile(PrintStream ps) {
        int qtdMethods = 0;

        //get the quantity of methods that gonna be used
        for (int aux = Variables.RANKING_METHODS; aux != 0; aux = aux >> 1) {
            if ((1 & aux) != 0) qtdMethods++;
        }

        ps.print("@relation trainingSet\n");
        int columns = (Variables.ATTRIBUTES_NUMBER - (Variables.FIELD_ID + 1)) * qtdMethods;
        for (int i = 0; i < columns; i++) {
            ps.print("@attribute att" + i + " numeric\n");
        }
        ps.print("@attribute isDuplicate numeric\n@data\n");
        ps.flush();
    }

    //create a new weka Instances
    public static Instances newInstances(String name) {
        Instances dataRaw;
        ArrayList<Attribute> atts = new ArrayList<Attribute>(Variables.getFieldsNumber() + 1);
        ArrayList<String> classVal = new ArrayList<String>(2);

        classVal.add("não-duplicata");
        classVal.add("duplicata");
        for (int i = 0; i < Variables.getFieldsNumber(); i++) {
            atts.add(new Attribute(Variables.ARFF_ATTRIBUTES_PREFIX + i));
        }
        atts.add(new Attribute("resultado", classVal));
        dataRaw = new Instances(name, atts, 0);
        dataRaw.setClassIndex(Variables.getFieldsNumber());
        return dataRaw;
    }


}
