package com.storminho.uffs;

public class GlobalVariables {
    //Where the tuple is gonna be split
    public static final String splitChars = ":+\\s*";

    //file to input path
    public static final String filePath = System.getenv("STORMINHO") + "cd-100.csv";

    //Which tuple's column holds the id field
    public static final int fieldId = 1;

    //Where Id field will be split
    public static final String indexSplitToken = "-";

    //Dup validator
    public static final String dupToken = "dup";

    //Weka
    public final static int attributesNumber = 12;
    public final static int rankingMethods = 4;
    public final static String arffPath = "out.arff";
}
