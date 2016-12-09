package com.model;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper;


public class ViewData {

    public static class DataMapper extends Mapper<Object,Text,NullWritable,Text> {

        private MultipleOutputs mos;


        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }
        
        public static ArrayList<String> createDatasetLabelled(ArrayList<String> values){
        	ArrayList<String> al = new ArrayList<String>();
            al.addAll(values.subList(2,3));
            al.addAll(values.subList(5, 6));
            al.addAll(values.subList(7,8));
            al.addAll(values.subList(957, 958));
            al.addAll(values.subList(963,968));
            al.addAll(values.subList(1015,1016));
            al.addAll(values.subList(1024,1026));
            al.addAll(values.subList(1036,1038));
            al.addAll(values.subList(1050,1053));
            al.addAll(values.subList(1058,1059));
            al.addAll(values.subList(1061,1063));
            al.addAll(values.subList(1073,1075));
            al.addAll(values.subList(1076,1077));
            al.addAll(values.subList(1082,1083));
            al.addAll(values.subList(1092,1098));
            al.addAll(values.subList(1101,1102));
            al.addAll(values.subList(26,27));
            return al;
        }
        
        public static ArrayList<String> createDatasetUnlabelled(ArrayList<String> values){
        	ArrayList<String> al = new ArrayList<String>();
        	al.addAll(values.subList(0,1));
            al.addAll(values.subList(2,3));
            al.addAll(values.subList(5, 6));
            al.addAll(values.subList(7,8));
            al.addAll(values.subList(957, 958));
            al.addAll(values.subList(963,968));
            al.addAll(values.subList(1015,1016));
            al.addAll(values.subList(1024,1026));
            al.addAll(values.subList(1036,1038));
            al.addAll(values.subList(1050,1053));
            al.addAll(values.subList(1058,1059));
            al.addAll(values.subList(1061,1063));
            al.addAll(values.subList(1073,1075));
            al.addAll(values.subList(1076,1077));
            al.addAll(values.subList(1082,1083));
            al.addAll(values.subList(1092,1098));
            al.addAll(values.subList(1101,1102));
            al.addAll(values.subList(26,27));
            return al;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
        	ArrayList<String> values = new ArrayList<String>(Arrays.asList(value.toString().split(",")));
        	String type = conf.get("data");
        	if(type.equals("labeled")) {
        		ArrayList<String> al = createDatasetLabelled(values);
	            if(!values.get(0).trim().equals("SAMPLING_EVENT_ID")) {
	                if(!StringUtils.isNumeric(al.get(31).trim())){
	                    al.set(31, "0");
	                }
	                    if(StringUtils.isNumeric(al.get(31).trim()) && (Integer.parseInt(al.get(31).trim())>0)){
	                    al.set(31, "1");
	                }
	                String val = al.toString();
	                val = val.substring(1,val.length()-1);
	                val = val.replaceAll("\\?", "0");
	                mos.write("data",NullWritable.get(), new Text(val));
	            }
	            else {
	                String val1 = al.toString();
	                val1 = val1.substring(1,val1.length()-1);
	                
	                mos.write("header",NullWritable.get(), new Text(val1));
	            }
            }
        	else {
        		ArrayList<String> al1 = createDatasetUnlabelled(values);
        		if(!values.get(0).trim().equals("SAMPLING_EVENT_ID")) {
	                
	                String val = al1.toString();
	                val = val.substring(1,val.length()-1);
	                val = val.replaceAll("\\?", "0");
	                mos.write("data",NullWritable.get(), new Text(val));
	            }
	            else {
	                String val1 = al1.toString();
	                val1 = val1.substring(1,val1.length()-1);
	                
	                mos.write("header",NullWritable.get(), new Text(val1));
	            }
        		
        	}
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }
        
}