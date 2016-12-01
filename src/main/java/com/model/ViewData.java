package com.model;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.model.WekaModel.WekaMapper;
import com.model.WekaModel.WekaReducer;

import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

public class ViewData {
	 public static void main(String[] args) throws Exception {

	        Configuration conf = new Configuration();
	        Job job0 = new Job(conf, "Parser call");
	        job0.setJarByClass(ViewData.class);
	        job0.setMapperClass(DataMapper.class);
	        job0.setReducerClass(DataReducer.class);
	        job0.setNumReduceTasks(0);
	        job0.setOutputKeyClass(NullWritable.class);
	        job0.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job0, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job0,
	                new Path(args[1]));
	        boolean ok = job0.waitForCompletion(true);
	        if (!ok) {
	            throw new Exception("Job failed");
	        }

	    }

	    public static class DataMapper extends Mapper<Object,Text,NullWritable,Text> {
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//	            context.write(NullWritable.get(), value);
	            ArrayList<String> values = new ArrayList<String>(Arrays.asList(value.toString().split(","))); 
	            ArrayList<String> al = new ArrayList<String>();
	            al.addAll(values.subList(0, 19));
	            al.addAll(values.subList(26,27));
	            al.addAll(values.subList(955, 1016));
	            al.addAll(values.subList(1018, values.size()));
	            context.write(NullWritable.get(), new Text(al.toString()));
	        }
	    }
	    
	    public static class DataReducer extends Reducer<NullWritable,Text,NullWritable,Text> {
	        public void map(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        	for(Text value : values)
	        		context.write(NullWritable.get(), value);
	        }
	    }
}
	     
