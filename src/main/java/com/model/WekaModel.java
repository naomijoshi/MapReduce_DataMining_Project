package com.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import weka.core.converters.ConverterUtils.DataSource;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by Naomi on 11/30/16.
 */
public class WekaModel {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapred.textoutputformat.separator", ":");
        if (otherArgs.length < 2) {
            System.err.println("Usage: PageRank <in> [<in>...] <out>");
            System.exit(2);
        }
        // Job to call the parser
        Job job0 = new Job(conf, "Parser call");
        job0.setJarByClass(WekaModel.class);
        job0.setMapperClass(WekaMapper.class);
        job0.setReducerClass(WekaReducer.class);
        job0.setOutputKeyClass(IntWritable.class);
        job0.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job0, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job0,
                new Path(otherArgs[1]));
        boolean ok = job0.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }

    }

    public static class WekaMapper extends Mapper<Object,Text,IntWritable,Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String val = value.toString();
        	val = val.substring(1, val.length()-1);
//        	System.out.println(val);
        	for(int i = 0; i<10; i++) {
        		double p = Math.random();
        		if(p<=0.5) {
        			context.write(new IntWritable(i), new Text(val));
        		}
        	}  
        }
    }


    public static class WekaReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	DataSource source = null;
            Instances instances = null;
            try {
            	for(Text value : values) {
            		source = new DataSource(value.toString());
            		instances = source.getDataSet();
            	}
            } catch (Exception e) {
                e.printStackTrace();
            }

            // setting class attribute if the data format does not provide this information
            // For example, the XRFF format saves the class attribute information as well
            if (instances.classIndex() == -1)
                instances.setClassIndex(instances.numAttributes() - 1);
            
         // Do 10-split cross validation
    		Instances[][] split = crossValidationSplit(instances,10);
     
    		// Separate split into training and testing arrays
    		Instances[] trainingSplits = split[0];
    		Instances[] testingSplits = split[1];
    		
    		Classifier models = new J48();
    		FastVector predictions = new FastVector();
    		for (int i = 0; i < trainingSplits.length; i++) {
    			Evaluation validation;
				try {
					validation = classify(models, trainingSplits[i], testingSplits[i]);
					predictions.appendElements(validation.predictions());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    		System.out.println(models.toString());
    		context.write(key, new Text(models.toString()));
        }
        
        public static Evaluation classify(Classifier model,
    			Instances trainingSet, Instances testingSet) throws Exception {
    		Evaluation evaluation = new Evaluation(trainingSet);
     
    		model.buildClassifier(trainingSet);
    		evaluation.evaluateModel(model, testingSet);
     
    		return evaluation;
    	}
        public static Instances[][] crossValidationSplit(Instances data, int numberOfFolds) {
    		Instances[][] split = new Instances[2][numberOfFolds];
     
    		for (int i = 0; i < numberOfFolds; i++) {
    			split[0][i] = data.trainCV(numberOfFolds, i);
    			split[1][i] = data.testCV(numberOfFolds, i);
    		}
     
    		return split;
    	}
    }

}
