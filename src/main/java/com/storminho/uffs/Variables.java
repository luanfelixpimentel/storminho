package com.storminho.uffs;

public class Variables {
    /* General ===============================================================*/
    //Path to project folder
    //Don't forget to define an environment variable called "STORMINHO" or something that you want (if you choose another name, you have to change in here
    public static final String projectPath = System.getenv("STORMINHO");
    //Path to arff folder
    public static final String arffPath = System.getenv("STORMINHO") + "/arff/";

    /* .csv related ==========================================================*/
    //Path to csv folder
    public static final String csvPath = System.getenv("STORMINHO") + "/csv/";
    //Where the tuple is gonna be split
    public static final String splitChars = ":+\\s*";
    //Which tuple's column holds the id field
    public static final int fieldId = 1;
    //how many columns does the csv have in total
    public final static int attributesNumber = 12;


    /* TrainingCreator =======================================================*/
    //How many lines the training set will have
    public static final int trainingLinesNumber = 10;
    //The csv file that will be used
    public static final String trainingCsvFile = "cd-100.csv";
    //Name of the output's file
    public static final String trainingOutputFile = "training.csv";

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
}
