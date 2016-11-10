package com.storminho.uffs;

public class GlobalVariables {
    //Where the tuple is gonna be split
    public static String splitChars = ":+\\s*";

    //file to input path
    public static String filePath = "/home/igorlemos/Downloads/storm-master/examples/storm-starter/src/main/java/com/microsoft/example/cd-100.csv";

    //Which tuple's column holds the id field
    public static int fieldId = 1;

    //Where Id field will be split
    public static String indexSplitToken = "-";

    //Dup validator
    public static String dupToken = "dup";

    static public int getFieldId() { return fieldId; }
    static public String getSplitChars() { return splitChars; }
    static public String getFilePath() { return filePath; }
    static public String getDupToken() { return dupToken; }
    static public String getIndexSplitToken() { return indexSplitToken; }

    //Weka
    public final static int attributesNumber = 12;
    public final static int rankingMethods = 4;
    public final static String arffPath = "out.arff";
}
