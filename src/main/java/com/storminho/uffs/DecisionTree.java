package com.storminho.uffs;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.task.TopologyContext;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;

import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.core.DenseInstance;

/**
 * Weka decision trees example.
 *
 */

public class DecisionTree extends BaseRichBolt implements IRichBolt {
    
    OutputCollector _collector;
    Instances trainingData;
    BufferedReader reader;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
     _collector = collector;
    
      try {
            reader = new BufferedReader(new FileReader(Variables.arffPath + Variables.trainingOutputFile));
            Instances trainingData = new Instances(reader);
            reader.close();
            trainingData.setClassIndex(trainingData.numAttributes() - 1);
            
          } catch (Exception ex) {
            System.out.println(ex);
        }
     }
    
    @Override
    public void execute(Tuple tuple) {
        DenseInstance ins = (DenseInstance)tuple.getValues().get(0);
        double result;
        
        J48 tree = new J48();
        String[] options = new String[1];
        options[0] = "-U"; 
        try {
            tree.setOptions(options);
        } catch (Exception ex) {
            System.out.println(ex);
          }
        try {
            tree.buildClassifier(trainingData);
        } catch (Exception ex) {
            System.out.println(ex);
          }
        
        //Print
        System.out.println(tree);

      //Predictions with test and training set of data
      BufferedReader datafile = reader; // colocar o datafile
      BufferedReader testfile = reader; //colocar o testfile

      Instances train = null;
        try {
            train = new Instances(reader);
        } catch (Exception ex) {
            System.out.println(ex);
          }
        
      trainingData.setClassIndex(trainingData.numAttributes() - 1);  // from somewhere. Datafile
      Instances test = null;
        try {
            test = new Instances(reader);
        } catch (Exception ex) {
            System.out.println(ex);
          }
        
      trainingData.setClassIndex(trainingData.numAttributes() - 1);    // from somewhere. Testfile
      
      // trainclassifier
      Classifier cls = new J48();
        try {
            cls.buildClassifier(train);
        } catch (Exception ex) {
            System.out.println(ex);
          }
        
      //evaluate classifier and print some statistics
      Evaluation eval = null;
        try {
            eval = new Evaluation(train);
        } catch (Exception ex) {
            System.out.println(ex);
          }
        
        try {
            eval.evaluateModel(cls, test);
        } catch (Exception ex) {
            System.out.println(ex);
          }
        
      System.out.println(eval.toSummaryString("\nResults\n======\n", false));
      
        for(int i=0;i<test.numInstances();i++){
            double value = 0;
            try {
                value = cls.classifyInstance(test.instance(i));
            } catch (Exception ex) {
                Logger.getLogger(DecisionTree.class.getName()).log(Level.SEVERE, null, ex);
            }
            String prediction=test.classAttribute().value((int)value); 
            System.out.println(test.instance(i)+"............Prediction.......... "+prediction); 
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("resposta_arvore", "resposta_certa"));
    }
    
     @Override
    public void cleanup() {
        try {
            reader.close();
        } catch (IOException ex) {
            Logger.getLogger(Arvore.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}