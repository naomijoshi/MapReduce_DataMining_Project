package com.model;

/**
 * Created by Naomi on 12/7/16.
 */
import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
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
                if (!f.getName().endsWith(".crc") && !f.getName().endsWith("_SUCCESS")) {
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
            System.out.println("model size: "+models.size());
            System.out.println("header is "+header);
        }

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            Instances testInstances = null;
            String line = "\n"+values.toString();
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
            options[1]="150";  //range of variables to make nominal

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
            testInstances1.setClass(testInstances1.attribute(149));

            int numTestInstances = testInstances1.numInstances();
            System.out.printf("There are %d test instances\n", numTestInstances);

            // Get the true class label from the instance's own classIndex.
            String trueClassLabel =
                    testInstances1.instance(0).toString(testInstances1.classIndex());
            Map<String,Integer> count = new HashMap<String, Integer>();
            Integer sum = 0;
            Integer max = 0;
            String p = null;
            for(int i=0;i<3;i++)
            {
//                System.out.println(models.get(i));
                try {
                    // Make the prediction here.
                    double predictionIndex = models.get(i).classifyInstance(testInstances1.instance(0));
                    System.out.println("prediction index " +predictionIndex);
                    // Get the predicted class label from the predictionIndex.
                    String predictedClassLabel =
                            testInstances1.classAttribute().value((int) predictionIndex);
//                    System.out.printf("predicted label"+
//                            predictedClassLabel);
                    if (count.containsKey(predictedClassLabel)){
                        sum = count.get(predictedClassLabel) + 1;
                        count.put(predictedClassLabel, sum);
                    }else
                    count.put(predictedClassLabel,1);

                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                for (Map.Entry<String,Integer> entry : count.entrySet()) {
                    if (entry.getValue() > max){
                        max = entry.getValue();
                        p = entry.getKey();
                    }
                }
            }
            context.write(new Text("SAMPLING ID"), new Text(p.toString()));


        }
    }
}
