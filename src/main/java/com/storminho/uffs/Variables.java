package com.storminho.uffs;

public class Variables {
    /* General ===============================================================*/
    //Path to project folder
    //Don't forget to define an environment variable called "STORMINHO" or something that you want (if you choose another name, you have to change in here
    public static final String projectPath = System.getenv("STORMINHO");
    //Path to arff folder
    public static final String arffPath = System.getenv("STORMINHO") + "/arff/";
    //Path to out folder
    public static final String outPath = System.getenv("STORMINHO") + "/out/";
    //Path to csv folder
    public static final String csvPath = System.getenv("STORMINHO") + "/csv/";

    /* .csv related ==========================================================*/
    //Where the tuple is gonna be split
    public static final String splitChars = ",";
    //Which tuple's column holds the id field
    public static final int fieldId = 0;
    //how many columns does the csv have in total
    public final static int attributesNumber = 18;


    /* TrainingCreator =======================================================*/
    //Name of the output's file
    public static final String trainingOutputFile = "trainingSet.arff";
    //Sample Size
    public static final double trainingSampleSize = 0.5;
    //Total de pares que vai ser processado
    public static final long totalPairs = 20000;
    //Duplicatas' total in dataset
    public static final long duplicatesTotal = 2;

    /*PairRanker =============================================================*/
    /*Select the methods that gonna be used. Use this as a sum with the following numbers:
    * 1 - Cosine Similarity
    * 2 - Jaccard  Similarity
    * 4 - Jaro Winkler Similarity
    * 8 - Levenshtein Similarity
    * 16 - Grams
    * Ex: public final static int rankingMethods = 2 + 8; means that Jaccard and Levenshtein gonna be used */
    public final static int rankingMethods = 1 + 2 + 4 + 8 + 16;
    //Where Id field will be split
    public static final String indexSplitToken = "-";
    //names used in attributes in arff files
    public static final String arffAttributesPrefix = "att";

    /* Counter ===============================================================*/
    public static final String counterOutputFile = "CONTAGEM";

    //how many fields the weka instances will have
    public static int getFieldsNumber() {
        return (attributesNumber - (fieldId + 1)) * countSim();
    }

    //count the true bits to count how many methods will be used
    static private int countSim() {
        int rm = Variables.rankingMethods, count = 0;
        for (; rm != 0; rm /= 2) {
            if ((1 & rm) != 0) count++;
        }
        return count;
    }
}
