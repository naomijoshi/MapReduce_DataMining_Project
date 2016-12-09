package com.model;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils.DataSource;
import weka.filters.Filter;
import weka.filters.supervised.attribute.AttributeSelection;
import weka.filters.unsupervised.attribute.NumericToNominal;
import weka.attributeSelection.CfsSubsetEval;
import weka.attributeSelection.GreedyStepwise;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.evaluation.NominalPrediction;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.functions.Logistic;
import weka.classifiers.lazy.IBk;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * Created by Naomi on 11/30/16.
 */
public class WekaModel {

    public static class WekaMapper extends Mapper<Object,Text,IntWritable,Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String val = value.toString();
            if(!val.contains("LATITUDE")){
                for(int i = 0; i<12; i++) {
                    double p = Math.random();
                    if(p<=0.2) {
                        context.write(new IntWritable(i), new Text(val));
                    }
                }
            }
        }
    }


    public static class WekaReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

		ArrayList<String> val;
        String header;

		public void setup(Context context) throws IOException, InterruptedException {
			URI[] files = context.getCacheFiles();
			BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
			String strLineRead = "";
			while ((strLineRead = br.readLine()) != null) {
				val = new ArrayList<String>(Arrays.asList(strLineRead.split(",")));
			}
			header = val.toString();
			header = header.substring(1,header.length()-1);
		}
		
		public static CharSequence iterable2str(Iterable<?> iterable, CharSequence delim){
			StringBuilder sb = new StringBuilder();
			for(Iterator<?> it = iterable.iterator(); it.hasNext(); ){
			    sb.append(it.next().toString()).append(delim);
			}
			return sb;
		    }
		
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Instances instances = null;
            Instances newData = null;
            Classifier models = null;
        	InputStream headerIS = new ByteArrayInputStream(header.getBytes("us-ascii"));
    		String content = "\n"+iterable2str(values,"\n").toString();
    		InputStream dataIS = new ByteArrayInputStream(content.getBytes("us-ascii"));
    		InputStream is = new SequenceInputStream(headerIS, dataIS);
    		CSVLoader cnv = new CSVLoader();
    		cnv.setSource(is);
    			try {
					instances = cnv.getDataSet();
				} catch (Exception e) {
					e.printStackTrace();
				}
		    is.close();
		    content = null;
		    dataIS.close();
		    headerIS.close();
		    
		    
		    
		    NumericToNominal convert= new NumericToNominal();
	        String[] options= new String[2];
	        options[0]="-R";
	        options[1]="32";  //range of variables to make numeric
	        

	        try {
				convert.setOptions(options);
				convert.setInputFormat(instances);
		        newData=Filter.useFilter(instances, convert);
		        
		        

            // setting class attribute if the data format does not provide this information
            if (newData.classIndex() == -1)
            	newData.setClassIndex(newData.numAttributes() - 1);
          
	        // Do 2-split cross validation
    		Instances[][] split = crossValidationSplit(newData,2);
     
    	// Separate split into training and testing arrays
    		Instances[] trainingSplits = split[0];
    		Instances[] testingSplits = split[1];
    		
    		models = new IBk(4);
    	// Select model to train based on key
//    		switch(key.get()%3) {
//    		case 0 : models = new J48();
//       		break;
//    		case 1: models = new IBk();
//    		break;
//    		case 2: models = new DecisionTable();
//    		break;
//    		case 3: models = new IBk();
//    		break;
//    		case 4: models = new RandomForest();
//    		break;
//    		}
    		
//    		System.out.println(newData.classAttribute()+":"+newData.classIndex());
    		
    		FastVector predictions = new FastVector();
    		for (int i = 0; i < trainingSplits.length; i++) {
    			Evaluation validation;
				validation = classify(models, trainingSplits[i], testingSplits[i]);
				predictions.appendElements(validation.predictions());
    		}
    		
    		double accuracy = calculateAccuracy(predictions);
//    		System.out.println("Accuracy of model "+models.getClass().getSimpleName()+" is "+accuracy);
				SerializationHelper.write("s3://naomi-hw2/output/models/"+models.getClass().getSimpleName()+key+".model", models);
	        } catch (Exception e) {
				e.printStackTrace();
			}
        }
        
        public static Evaluation classify(Classifier model, Instances trainingSet, Instances testingSet) throws Exception {
//        	System.out.println("Inside the classifier");
    		Evaluation evaluation = new Evaluation(trainingSet);
    		model.buildClassifier(trainingSet);
    		evaluation.evaluateModel(model, testingSet);
    		return evaluation;
    	}
        
        public static Instances[][] crossValidationSplit(Instances data, int numberOfFolds) {
//        	System.out.println("Inside crossValidationSplit");
    		Instances[][] split = new Instances[2][numberOfFolds];
    		for (int i = 0; i < numberOfFolds; i++) {
    			split[0][i] = data.trainCV(numberOfFolds, i);
    			split[1][i] = data.testCV(numberOfFolds, i);
    		}
    		return split;
    	}
        
        public static double calculateAccuracy(FastVector predictions) {
//        	System.out.println("Inside calculateAccuracy");
    		double correct = 0;
    		for (int i = 0; i < predictions.size(); i++) {
    			NominalPrediction np = (NominalPrediction) predictions.elementAt(i);
    			if (np.predicted() == np.actual()) {
    				correct++;
    			}
    		}
    		return 100 * correct / predictions.size();
    	}
    }

}
