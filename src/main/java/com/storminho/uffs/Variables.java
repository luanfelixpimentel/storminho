package com.storminho.uffs;

public class Variables {
    /* General ===============================================================*/
    //Path to project folder
    public static final String projectPath = System.getenv("STORMINHO");
    //Path to csv folder
    public static final String csvPath = System.getenv("STORMINHO") + "/csv/";
    //Path to arff folder
    public static final String arffPath = System.getenv("STORMINHO") + "/arff/";

    /* TrainingCreator =======================================================*/
    //How many lines the training set will have
    public static final int trainingLinesNumber = 10;
    //The csv file that will be used
    public static final String trainingCsvFile = "cd-100.csv";
    //Name of the output's file
    public static final String trainingOutputFile = "training.csv";
}
