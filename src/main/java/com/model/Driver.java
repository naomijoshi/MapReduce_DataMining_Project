package com.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
//      Job to call the parser
       conf.set("data", "labeled");
       long milliSeconds = 1000*60*60*3;
       conf.setLong("mapred.task.timeout", milliSeconds);
       Job job = Job.getInstance(conf, "Extract header");
       job.setJarByClass(Driver.class);
       job.setMapperClass(ViewData.DataMapper.class);
       job.setOutputKeyClass(NullWritable.class);
       job.setOutputValueClass(Text.class);
       job.setNumReduceTasks(0);
       FileInputFormat.addInputPath(job, new Path(otherArgs[0]+"/labeled.csv.bz2"));
       FileOutputFormat.setOutputPath(job,
               new Path(otherArgs[1]+"/labeledData"));
       MultipleOutputs.addNamedOutput(job, "header", TextOutputFormat.class, NullWritable.class, Text.class);
       MultipleOutputs.addNamedOutput(job, "data", TextOutputFormat.class, NullWritable.class, Text.class);
       boolean ok = job.waitForCompletion(true);
       if (!ok) {
           throw new Exception("Job failed");
       }

////      Job to call the parser
       Job job1 = Job.getInstance(conf, "Train model");
       job1.setJarByClass(Driver.class);
       job1.setMapperClass(WekaModel.WekaMapper.class);
       job1.setReducerClass(WekaModel.WekaReducer.class);
       job1.addCacheArchive(new Path(otherArgs[1]+"/labeledData/header-m-00000").toUri());
       job1.setOutputKeyClass(IntWritable.class);
       job1.setOutputValueClass(Text.class);
       FileInputFormat.addInputPath(job1, new Path(otherArgs[1]+"/labeledData"));
       FileOutputFormat.setOutputPath(job1,
               new Path(otherArgs[1]+"/arff"));
       if (!job1.waitForCompletion(true)) {
           throw new Exception("Job failed");
       }

//       Job to call the parser
         conf.set("data", "unlabeled");
	      Job job2 = Job.getInstance(conf, "Extract header");
	      job2.setJarByClass(Driver.class);
	      job2.setMapperClass(ViewData.DataMapper.class);
	      job2.setOutputKeyClass(NullWritable.class);
	      job2.setOutputValueClass(Text.class);
	      job2.setNumReduceTasks(0);
	      FileInputFormat.addInputPath(job2, new Path(otherArgs[0]+"/unlabeled.csv.bz2"));
	      FileOutputFormat.setOutputPath(job2,
	              new Path(otherArgs[1]+"/unlabeledData"));
	      MultipleOutputs.addNamedOutput(job2, "header", TextOutputFormat.class, NullWritable.class, Text.class);
	      MultipleOutputs.addNamedOutput(job2, "data", TextOutputFormat.class, NullWritable.class, Text.class);
	      if (!job2.waitForCompletion(true)) {
	          throw new Exception("job2 failed");
	      }
        
//      //Job to predict unlabeled data
      Job job4 = new Job(conf, "Predict data");
      job4.setJarByClass(Driver.class);
      job4.setMapperClass(Prediction.PredictionMapper.class);
      job4.setNumReduceTasks(0);
      job4.addCacheArchive(new Path(otherArgs[1]+"/unlabeledData/header-m-00000").toUri());
      job4.addCacheArchive(new Path(otherArgs[1]+"/models/").toUri());
      job4.setOutputKeyClass(IntWritable.class);
      job4.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job4, new Path(otherArgs[1]+"/unlabeledData"));
      FileOutputFormat.setOutputPath(job4,
              new Path(otherArgs[1]+"/predicted"));
      if (!job4.waitForCompletion(true)) {
          throw new Exception("Job failed");
      }
    }
}
