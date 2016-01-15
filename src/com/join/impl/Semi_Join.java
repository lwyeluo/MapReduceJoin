package com.join.impl;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.example.MyApp.MyController;


import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jasper.tagplugins.jstl.core.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Semi_Join extends Configured implements Tool
{
	  public static final String DELIMITER = "\t"; // 字段分隔符
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text>
    {

        // 用于缓存user_id文件中的数据
        private HashSet<String> userIds = new HashSet<String>();
        
        private Text key = new Text();
        private Text value = new Text();
        private String[] keyValue;

        // 此方法会在map方法执行之前执行
        @Override
			  protected void setup(Context context) throws IOException, InterruptedException
			        {
			        	 BufferedReader in = null;  
			        	try  
			            {  
			                // 从当前作业中获取要缓存的文件  
			        		 URI[] paths = Job.getInstance(context.getConfiguration()).getCacheFiles();
			                 String userId = null;  
			                for (URI path : paths)  
			                { 
			                	System.out.println(path.toString());
			                    if (path.toString().contains("user"))  
			                    {  
			                        in = new BufferedReader(new FileReader(path.toString()));  
			                        while (null != (userId = in.readLine()))  
			                        {  
			                            String[] splits = userId.split(DELIMITER);
			                            if(splits.length < 2) continue;
			                            userIds.add(splits[0]);
			                        }  
			                    }  
			                }  
			            }  
			            catch (IOException e)  
			            {  
			                e.printStackTrace();  
			            }  
			            }  
			        

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
        	FileSplit split = (FileSplit) context.getInputSplit();
            String filePath = split.getPath().toString();
            // 获取记录字符串
            String line = value.toString();
      //      System.out.println(userIds);
            //System.out.println(line);
            // 抛弃空记录
            if (line == null || line.trim().equals("")) return;
            String[] values = line.split(DELIMITER);
            // 在map阶段过滤掉不需要的数据
            if(userIds.contains( values[0]))
            {
            if (filePath.contains("user.txt")) {
                if (values.length < 2)  return;
          //   System.out.println(values[1]);
                context.write(new Text(values[0]), new Text("u#" + values[1]));
            }
            // 处理login_logs.txt数据
            else if (filePath.contains("login_logs.txt")) {
                if (values.length < 3)  return;
       //System.out.println(values[2]);               
                context.write(new Text(values[0]), new Text("l#" + values[1] + DELIMITER + values[2]));
            }
            }
       
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {

    	 public  void reduce(Text key, Iterable<Text> values,
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
    
    public int run(String[] args) throws Exception
    {
    	   Configuration conf=new Configuration();        
    	String[] otherArgs = new String[]{"*.txt","output3","user.txt"}; 
    	   if (otherArgs.length!=3) {
               System.err.println("Usage:invertedindex<in><out>");
               System.exit(2);
           }   
    	//每次运行前删除输出目录    
        
           Path outputPath = new Path(otherArgs[1]);
           outputPath.getFileSystem(conf).delete(outputPath, true);
    	
        Job job = Job.getInstance(getConf(), "Semi_Join");
        
        job.setJobName("Semi_Join");
        job.setJarByClass(Semi_Join.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

     
        // 我们把第一个参数的地址作为要缓存的文件路径
        job.addCacheFile(new URI(otherArgs[2]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
    	   long startTime = System.currentTimeMillis();
    	int res = ToolRunner.run(new Configuration(), new Semi_Join(), args);  
    	System.out.println("用时为"+(System.currentTimeMillis()-startTime));
        System.exit(res);
    }

}
