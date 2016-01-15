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
			        	 String keyCatch = context.getConfiguration().get("keyCatch");
			         	int keyCatch_1 = Integer.parseInt(keyCatch);
			         	String d = context.getConfiguration().get("d");
			        	try  
			            {  
			                // 从当前作业中获取要缓存的文件  
			        		 URI[] paths = Job.getInstance(context.getConfiguration()).getCacheFiles();
			                 String userId = null;  
			                for (URI path : paths)  
			                { 
			                	System.out.println(path.toString());
			                    if (path.toString().contains(d))  
			                    {  
			                        in = new BufferedReader(new FileReader(path.toString()));  
			                        while (null != (userId = in.readLine()))  
			                        {  
			                            String[] splits = userId.split(DELIMITER);
			                            if(splits.length < 2) continue;
			                            userIds.add(splits[ keyCatch_1]);
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
            
            String a = context.getConfiguration().get("a");
        	String a1 = context.getConfiguration().get("a1");
        	int a_1 = Integer.parseInt(a1);
        	String b = context.getConfiguration().get("b");
        	String b1 = context.getConfiguration().get("b1");
        	int b_1 = Integer.parseInt(b1);
            // 获取记录字符串
            String line = value.toString();
      //      System.out.println(userIds);
            //System.out.println(line);
            // 抛弃空记录
            if (line == null || line.trim().equals("")) return;
            String[] values = line.split(DELIMITER);
          
        
            
            if (filePath.contains(a)) {
                if (values.length < 2)  return;
                // 在map阶段过滤掉不需要的数据
                if(userIds.contains( values[a_1]))
                {
                	  String Key_a= values[a_1];
                      String Value_a="";
                      for(int i=0;i<values.length;i++)
                      {
                      	if (i != a_1)
                      	Value_a+=values[i]+DELIMITER;           		
                      }
                      Value_a=Value_a.trim();
                      context.write(new Text(Key_a), new Text("u#" + Value_a));
                }
        
            }
            // 处理login_logs.txt数据
            else if (filePath.contains(b)) {
                if (values.length < 2)  return;
                // 在map阶段过滤掉不需要的数据
                if(userIds.contains( values[b_1]))
                {
                	 String Key_b= values[b_1];
                     String Value_b="";
                     for(int i=0;i<values.length;i++)
                     {
                     	if (i != b_1)
                     	Value_b+=values[i]+DELIMITER;           		
                     }
                     Value_b=Value_b.trim();
                     context.write(new Text(Key_b), new Text("l#" + Value_b));
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
    	String[] otherArgs = new String[]{"input3","output3","user.txt"}; 
    	   if (otherArgs.length!=3) {
               System.err.println("Usage:invertedindex<in><out>");
               System.exit(2);
           }   
    	//每次运行前删除输出目录    
        
           Path outputPath = new Path(otherArgs[1]);
           outputPath.getFileSystem(conf).delete(outputPath, true);
    	
           //需要加载的小表
           otherArgs[2] =args[7];
   	    	String keyCatch="";
           	if(args[0].equals(args[7]))
           	{
           		keyCatch=args[1];
           	}
           	else if(args[2].equals(args[7]))
           	{
           		keyCatch=args[3];
           	}     	
            conf.set("a",args[0]);
  		  	conf.set("a1",args[1]);
  		  	conf.set("b",args[2]);
  		  	conf.set("b1",args[3]);
       		  conf.set("keyCatch",keyCatch);
       		  conf.set("d",args[7]);
           
           
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
