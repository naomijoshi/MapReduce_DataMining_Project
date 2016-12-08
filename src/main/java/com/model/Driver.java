package com.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by Naomi on 12/3/16.
 */
public class Driver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: PageRank <in> [<in>...] <out>");
            System.exit(2);
        }
        // Job to process labeled data
//        Job job0 = new Job(conf, "Extract header");
//        job0.setJarByClass(Driver.class);
//        job0.setMapperClass(ViewData.DataMapper.class);
//        job0.setOutputKeyClass(NullWritable.class);
//        job0.setOutputValueClass(Text.class);
//        job0.setNumReduceTasks(0);
//        FileInputFormat.addInputPath(job0, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job0,
//                new Path(otherArgs[1]+"/processedData"));
//        MultipleOutputs.addNamedOutput(job0, "header", TextOutputFormat.class, NullWritable.class, Text.class);
//        MultipleOutputs.addNamedOutput(job0, "data", TextOutputFormat.class, NullWritable.class, Text.class);
//        boolean ok = job0.waitForCompletion(true);
//        if (!ok) {
//            throw new Exception("Job failed");
//        }

        // Job to call the parser
//        Job job1 = new Job(conf, "Train model");
//        job1.setJarByClass(Driver.class);
//        job1.setMapperClass(WekaModel.WekaMapper.class);
//        job1.setReducerClass(WekaModel.WekaReducer.class);
//        job1.addCacheFile(new Path(otherArgs[1]+"/processedData/header-m-00000").toUri());
//        job1.setOutputKeyClass(IntWritable.class);
//        job1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job1, new Path(otherArgs[1]+"/processedData"));
//        FileOutputFormat.setOutputPath(job1,
//                new Path(otherArgs[1]+"/arff"));
//        if (!job1.waitForCompletion(true)) {
//            throw new Exception("Job failed");
//        }

        //Job to process unlabeled data
//        Job job3 = new Job(conf, "Preprocess unlabeled data");
//        job3.setJarByClass(Driver.class);
//        job3.setMapperClass(ViewData.DataMapper.class);
//        job3.setOutputKeyClass(NullWritable.class);
//        job3.setOutputValueClass(Text.class);
//        job3.setNumReduceTasks(0);
//        FileInputFormat.addInputPath(job3, new Path(otherArgs[2]));
//        FileOutputFormat.setOutputPath(job3,
//                new Path(otherArgs[1]+"/predictedData"));
//        MultipleOutputs.addNamedOutput(job3, "header", TextOutputFormat.class, NullWritable.class, Text.class);
//        MultipleOutputs.addNamedOutput(job3, "data", TextOutputFormat.class, NullWritable.class, Text.class);
//        boolean ok = job3.waitForCompletion(true);
//        if (!ok) {
//            throw new Exception("Job failed");
//        }
//
//        //Job to predict unlabeled data
        Job job4 = new Job(conf, "Predict data");
        job4.setJarByClass(Driver.class);
        job4.setMapperClass(Prediction.PredictionMapper.class);
        job4.setNumReduceTasks(0);
        job4.addCacheFile(new Path(otherArgs[1]+"/predictedData/header-m-00000").toUri());
        job4.addCacheArchive(new Path(otherArgs[1]+"/models/").toUri());
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(otherArgs[1]+"/predictedData/test.csv"));
        FileOutputFormat.setOutputPath(job4,
                new Path(otherArgs[1]+"/predicted"));
        if (!job4.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }

    }
}
