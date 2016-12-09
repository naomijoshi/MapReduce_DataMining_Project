package com.model;

/**
 * Created by Naomi on 12/7/16.
 */
import java.io.*;
import java.net.URI;
import java.util.*;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import weka.classifiers.Classifier;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;

public class Prediction
{
    public static class PredictionMapper extends Mapper<Object, Text, Text, Text> {
        ArrayList<Classifier> models;
        String header;
        public void setup(Context context) throws IOException
        {
            models = new ArrayList<Classifier>();
            Classifier classifier = null;
            File[] files = new File("./models").listFiles();
            for (File f:files){
                if (f.getName().endsWith(".model") ) {
                    try {
                        classifier = (Classifier) weka.core.SerializationHelper.read(f.toString());
                        } catch (Exception e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        models.add(classifier);
                    }

                }
            URI[] Hfiles = context.getCacheFiles();
            BufferedReader br = new BufferedReader(new FileReader(Hfiles[0].getPath()));
            String strLineRead = "";
            while ((strLineRead = br.readLine()) != null) {
                header = strLineRead;
            }
            ArrayList<String> input = new ArrayList<String>(Arrays.asList(header.split(",")));
            input.remove(0);
            String temp = input.toString();
            header = temp.substring(1, temp.length()-1);
        }

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            Instances testInstances = null;
            ArrayList<String> input = new ArrayList<String>(Arrays.asList(values.toString().split(",")));
            String sampleID = input.get(0);
            input.remove(0);
            String temp = input.toString();
            String line = "\n"+temp.substring(1, temp.length()-1);
            if (line.contains("LATITUDE")) return;
            InputStream headerIS = new ByteArrayInputStream(header.getBytes("us-ascii"));
            InputStream dataIS = new ByteArrayInputStream(line.getBytes("us-ascii"));
            InputStream is = new SequenceInputStream(headerIS, dataIS);
            CSVLoader cnv = new CSVLoader();
            cnv.setSource(is);
            try {
                testInstances = cnv.getDataSet();
            } catch (Exception e) {
                e.printStackTrace();
            }
            is.close();
            dataIS.close();
            headerIS.close();

            NumericToNominal convert= new NumericToNominal();
            String[] options= new String[2];
            options[0]="-R";
            options[1]="32";  //range of variables to make nominal

            Instances testInstances1 =null;
            try {
                convert.setOptions(options);
                convert.setInputFormat(testInstances);
                testInstances1 = Filter.useFilter(testInstances, convert);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            // Mark the last attribute in each instance as the true class.
            testInstances1.setClassIndex(testInstances1.numAttributes()-1);

            int numTestInstances = testInstances1.numInstances();


            Integer countZero = 0;
            Integer countOne = 0;
            String p = null;
            for(int i=0;i<models.size();i++)
            {
                try {
                    // Make the prediction here.
                    double predictionIndex = models.get(i).classifyInstance(testInstances1.instance(0));
                    if(predictionIndex == 0.0)
                    	countZero++;
                    else
                    	countOne++;
                    
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
               
            }
            
            if(countOne>=countZero)
            	context.write(new Text(sampleID), new Text("1"));
            else
            	context.write(new Text(sampleID), new Text("0"));
        }
    }
}