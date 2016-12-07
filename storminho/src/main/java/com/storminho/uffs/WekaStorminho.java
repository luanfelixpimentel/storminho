package com.storminho.uffs;

import java.util.ArrayList;
import weka.core.Attribute;
import weka.core.Instances;

public class WekaStorminho {
    public Instances newInstances(String name) {
        Instances dataRaw;
        ArrayList<Attribute> atts = new ArrayList<Attribute>(Variables.getFieldsNumber() + 1);
        ArrayList<String> classVal = new ArrayList<String>(2);

        classVal.add("não-duplicata");
        classVal.add("duplicata");
        for (int i = 0; i < Variables.getFieldsNumber(); i++) {
            atts.add(new Attribute("att" + i));
        }
        atts.add(new Attribute("resultado", classVal));
        dataRaw = new Instances(name, atts, 0);
        dataRaw.setClassIndex(Variables.getFieldsNumber());
        return dataRaw;
    }
}
