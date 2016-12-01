package com.storminho.uffs;

import java.io.BufferedReader;
import java.io.FileReader;
import weka.classifiers.trees.J48;
import weka.core.Instances;


public class teste {
    public static void main (String[] args) {
        J48 arv;
        Instances data;
        BufferedReader reader;
        data = null;
        try {
            reader = new BufferedReader(new FileReader(Variables.arffPath + Variables.trainingOutputFile));
            System.out.println("checkcheckpizza1" + reader);
            data = new Instances(reader);
            System.out.println("checkcheckpizza2" + data);
            data.setClassIndex(data.numAttributes() - 1);
            System.out.println("checkcheckpizza3");
            arv = new J48();
            System.out.println("checkcheckpizza4");
            arv.setUnpruned(true);
            System.out.println("checkcheckpizza5");
            arv.buildClassifier(data);
            System.out.println("checkcheckpizza6");
        } catch (Exception ex) {
            System.out.println(ex);
        }

    }
}
