package com.storminho.uffs;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;


public class DecisionTree {
    

    public static BufferedReader readDataFile(String filename) {
        BufferedReader inputReader = null;

         try {
             inputReader = new BufferedReader(new FileReader(filename));
          } catch (FileNotFoundException ex) {
             System.err.println("File not found: " + filename);
         }
         return inputReader;
}

public static void main(String[] args) throws Exception {

        //Get File
        BufferedReader reader = readDataFile("maitre.txt");

       //Get the data
       Instances data = new Instances(reader);
       reader.close();

       //Setting class attribute 
       data.setClassIndex(data.numAttributes() - 1);

      //Make tree
      J48 tree = new J48();
      String[] options = new String[1];
      options[0] = "-U"; 
      tree.setOptions(options);
      tree.buildClassifier(data);

      //Print tree
      System.out.println(tree);

      //Predictions with test and training set of data

      BufferedReader datafile = readDataFile("maitre.txt");
      BufferedReader testfile = readDataFile("maitretest.txt");

      Instances train = new Instances(datafile);
      data.setClassIndex(data.numAttributes() - 1);  // from somewhere
      Instances test = new Instances(testfile);
      data.setClassIndex(data.numAttributes() - 1);    // from somewhere
      // train classifier
      Classifier cls = new J48();
      cls.buildClassifier(train);
      // evaluate classifier and print some statistics
      Evaluation eval = new Evaluation(train);
      eval.evaluateModel(cls, test);
      System.out.println(eval.toSummaryString("\nResults\n======\n", false));
    
    
    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("", ""));
    }

    @Override
    public void cleanup() {

    }
}
}