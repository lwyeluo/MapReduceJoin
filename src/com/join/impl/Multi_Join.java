package com.join.impl;
import java.io.IOException;

import java.util.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;


public class Multi_Join {

	 public static final String DELIMITER = "\t"; // 字段分隔符

	    static class MyMappper extends Mapper<LongWritable, Text, Text, Text> {
	        @Override
	        protected void map(LongWritable key, Text value,
	                Mapper<LongWritable, Text, Text, Text>.Context context)
	                throws IOException, InterruptedException {
	        	String c = context.getConfiguration().get("c");
	        	String c1 = context.getConfiguration().get("c1");
	        	int c_1 = Integer.parseInt(c1);
	        	String b2 = context.getConfiguration().get("b2");
	        	int b_2 = Integer.parseInt(b2);
	            FileSplit split = (FileSplit) context.getInputSplit();
	            String filePath = split.getPath().toString();
	            // 获取记录字符串
	            String line = value.toString();
	            //System.out.println(line);
	            // 抛弃空记录
	            if (line == null || line.trim().equals("")) return;

	            String[] values = line.split(DELIMITER);
	            // 处理user.txt数据
	            if (filePath.contains("part-r-00000")) {
	            	   if (values.length < 2)  return;
	                   //System.out.println(values[1]);
	                   String Key_b= values[b_2];
	                   String Value_b="";
	                   for(int i=0;i<values.length;i++)
	                   {
	                   	if (i != b_2)
	                   	Value_b+=values[i]+DELIMITER;           		
	                   }
	                   Value_b=Value_b.trim();
	                   context.write(new Text(Key_b), new Text("u#" + Value_b));
	            }
	            // 处理login_logs.txt数据
	            else if (filePath.contains(c)) {
	            	   if (values.length < 2)  return;
	                   //System.out.println(values[1]);
	                   String Key_c= values[c_1];
	                   String Value_c="";
	                   for(int i=0;i<values.length;i++)
	                   {
	                   	if (i != c_1)
	                   	Value_c+=values[i]+DELIMITER;           		
	                   }
	                   Value_c=Value_c.trim();
	                   context.write(new Text(Key_c), new Text("l#" + Value_c));
	            }
	        }
	    }

	    static class MyReducer extends Reducer<Text, Text, Text, Text> {
	        @Override
	        protected void reduce(Text key, Iterable<Text> values,
	                Reducer<Text, Text, Text, Text>.Context context)
	                throws IOException, InterruptedException {

	            LinkedList<String> linkU = new LinkedList<String>();  //users值
	            LinkedList<String> linkL = new LinkedList<String>();  //login_logs值
	              
	            for (Text tval : values) {
	                String val = tval.toString();  
	                if(val.startsWith("u#")) {
	                    linkU.add(val.substring(2));
	                } else if(val.startsWith("l#")) {
	                    linkL.add(val.substring(2));
	                }
	            }
	              
	            for (String u : linkU) {
	                for (String l : linkL) {
	                    context.write(key, new Text(u + DELIMITER + l));
	                }
	            }
	        }
	    }

	    public int run(String[] args) throws Exception {
		
		 Configuration conf=new Configuration();     
	        String[] otherArgs=new String[]{"input3","output3","output_tmp"}; 
	          conf.set("b2",args[4]);
			  conf.set("c",args[5]);
			  conf.set("c1",args[6]);
	    //每次运行前删除输出目录    
	        Path outputPath = new Path(otherArgs[1]);
	        outputPath.getFileSystem(conf).delete(outputPath, true);
		
		ToolRunner.run(new Multi_Join_test(), args);
		Job job = new Job(conf, "Multiple Table Join");
		
		job.setJarByClass(Multi_Join.class);

		// 设置Map和Reduce处理类
        job.setMapperClass(MyMappper.class);
        job.setReducerClass(MyReducer.class);
		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]+"/" + args[5]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		    return job.waitForCompletion(true) ? 0 : 1;
	}
}