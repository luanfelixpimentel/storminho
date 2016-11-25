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
    public static final String splitChars = ":+\\s*";
    //Which tuple's column holds the id field
    public static final int fieldId = 1;
    //how many columns does the csv have in total
    public final static int attributesNumber = 12;


    /* TrainingCreator =======================================================*/
    //Name of the output's file
    public static final String trainingOutputFile = "trainingSet.arff";
    //Sample Size
    public static final double trainingSampleSize = 0.05;
    //Total de pares que vai ser processado
    public static final long totalPairs = 120000;
    //Duplicatas' total in dataset
    public static final long duplicatesTotal = 10000;

    /*PairRanker =============================================================*/
    /*Select the methods that gonna be used. Use this as a sum with the following numbers:
    * 1 - Cosine Similarity
    * 2 - Jaccard  Similarity
    * 4 - Jaro Winkler Similarity
    * 8 - Levenshtein Similarity
    * Ex: public final static int rankingMethods = 2 + 8; means that Jaccard and Levenshtein gonna be used */
    public final static int rankingMethods = 1 + 8;
    //Where Id field will be split
    public static final String indexSplitToken = "-";

    /* Counter ===============================================================*/
    public static final String counterOutputFile = "CONTAGEM";
}
