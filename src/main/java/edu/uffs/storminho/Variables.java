package edu.uffs.storminho;

public class Variables {
    /* TrainingCreator =======================================================*/
    //Name of the output's file
    public static final String TRAININGSET_OUTPUT_FILE = "trainingSet.arff";
    //Sample Size = Essa porcentagem define quantos pares serão selecionado dentro do conjunto de pares positivos
    public static final double SAMPLE_SIZE = 0.3;
    //Quantas duplicatas existem no conjunto de teste
    public static final int TOTAL_DUPLICATAS = 1000;
    //Quantos pares tem ao todo
    public static final int TOTAL_PARES = 417000;
    //==========================================================================

    /* .csv related ==========================================================*/
    //Where the tuple is gonna be split
    public static final String SPLIT_CHARS = ",";
    //Which tuple's column holds the id field
    public static final int FIELD_ID = 0;
    //how many columns does the csv have in total
    public final static int ATTRIBUTES_NUMBER = 18;
    //Arquivo que será usado no LineSpout
    public final static String DATASET_INPUT = "teste01";
    //==========================================================================

    /*PairGenerator ==========================================================*/
    //Tamanho máximo de um set pra realizar as combinações
    public static final int MAX_SET_SIZE = 50;
    //==========================================================================

    /* General ===============================================================*/
    //Path to project folder
    //Don't forget to define an environment variable called "STORMINHO" or something that you want (if you choose another name, you have to change in here
    public static final String PROJECT_PATH = System.getenv("STORMINHO");
    //Path to arff folder
    public static final String ARFF_PATH = System.getenv("STORMINHO") + "/arff/";
    //Path to out folder
    public static final String OUT_PATH = System.getenv("STORMINHO") + "/out/";
    //Path to csv folder
    public static final String CSV_PATH = System.getenv("STORMINHO") + "/csv/";
    //==========================================================================

    /*PairRanker =============================================================*/
    /*Select the methods that gonna be used. Use this as a sum with the following numbers:
    * 1 - Cosine Similarity
    * 2 - Jaccard  Similarity
    * 4 - Jaro Winkler Similarity
    * 8 - Levenshtein Similarity
    * 16 - Grams
    * Ex: public final static int RANKING_METHODS = 2 + 8; means that Jaccard and Levenshtein gonna be used */
    public final static int RANKING_METHODS = 8;
    //Where Id field will be split
    public static final String ID_SPLIT_CHARS = "-";
    //names used in attributes in arff files
    public static final String ARFF_ATTRIBUTES_PREFIX = "att";
    //==========================================================================

    /*Debugs and Preparation =================================================*/
    //if true, will only proccess the necessary to count how many pairs and duplicatas there is in a set
    public static boolean COUNT_MODE;
    //==========================================================================

    //how many fields the weka instances will have
    public static int getFieldsNumber() {
        return (ATTRIBUTES_NUMBER - (FIELD_ID + 1)) * countSim();
    }

    //count the true bits to count how many methods will be used
    static private int countSim() {
        int rm = Variables.RANKING_METHODS, count = 0;
        for (; rm != 0; rm /= 2) {
            if ((1 & rm) != 0) count++;
        }
        return count;
    }
}
