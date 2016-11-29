package com.storminho.uffs;

import java.util.ArrayList;
import weka.core.Attribute;
import weka.core.Instances;

public class WekaStorminho {
    public Instances newInstances(String name) {
        Instances dataRaw;
        ArrayList<Attribute> atts = new ArrayList<>(Variables.getFieldsNumber() + 1);
        ArrayList<String> classVal = new ArrayList<>(2);

        classVal.add("1");
        classVal.add("0");
        for (int i = 0; i < Variables.getFieldsNumber(); i++) {
            atts.add(new Attribute("att" + i));
        }
        atts.add(new Attribute("resultado", classVal));
        dataRaw = new Instances(name, atts, 0);
        dataRaw.setClassIndex(dataRaw.numAttributes() - 1);
        return dataRaw;
    }
}
