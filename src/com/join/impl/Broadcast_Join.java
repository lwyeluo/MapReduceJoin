package com.join.impl;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.commons.collections.map.HashedMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Broadcast_Join extends Configured implements Tool {
    public static final String DELIMITER = "\t"; // 字段分隔符

    static class MyMappper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> userMaps = new HashedMap();
        
        @Override
        protected void setup(Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException ,InterruptedException {
            //可以通过localCacheFiles获取本地缓存文件的路径
            //Configuration conf = context.getConfiguration();
            //Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(conf);
            
            //此处使用快捷方式users.txt访问
        	String keyCatch = context.getConfiguration().get("keyCatch");
        	int keyCatch_1 = Integer.parseInt(keyCatch);
        	String d = context.getConfiguration().get("d");
        	
            FileReader fr = new FileReader(d);
            BufferedReader br = new BufferedReader(fr);
            String line;
            
            while((line = br.readLine()) != null) {
                //map端加载缓存数据
                String[] splits = line.split(DELIMITER);
                if(splits.length < 2) continue;
                String Value_a="";
          	  for(int i=0;i<splits.length;i++)
                {
                	if (i !=keyCatch_1)
                	Value_a+=splits[i]+DELIMITER;           		
                }
          	   Value_a=Value_a.trim();
          	   String Value= Value_a; 
                if(userMaps.containsKey(splits[keyCatch_1]))
                	{
                	String tmp= userMaps.get(splits[0]);              	
                	Value=tmp +DELIMITER+ Value_a;
                	}
                userMaps.put(splits[keyCatch_1], Value);
            }
        };
        
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // 获取记录字符串
        	   FileSplit split = (FileSplit) context.getInputSplit();
               String filePath = split.getPath().toString();
         
            String line = value.toString();
        	String fileIn= context.getConfiguration().get("fileIn");
        	String keyIn = context.getConfiguration().get("keyIn");
        	int keyIn_1 = Integer.parseInt(keyIn);
            // 抛弃空记录
            if (line == null || line.trim().equals("")) return;
            String[] values = line.split(DELIMITER);
            if (filePath.contains(fileIn)) 
            {
            	if(values.length < 2) return;
            String name = userMaps.get(values[keyIn_1]);
            Text t_key = new Text(values[keyIn_1]);
            if(name==null) return; 
            String[] tmp= name.split(DELIMITER);
            if(tmp.length<2)
            {
            String Value_a="";
            for(int i=0;i<values.length;i++)
            {
            	if (i != keyIn_1)
            	Value_a+=values[i]+DELIMITER;           		
            }
            Value_a=Value_a.trim();
            Text t_value = new Text(name + DELIMITER + Value_a);
            context.write(t_key, t_value);
            }
            else
            {
            	for(int i =0;i<tmp.length;i++)
            	{
            		String Value_a="";
                    for(int j=0;j<values.length;j++)
                    {
                    	if (j != keyIn_1)
                    	Value_a+=values[j]+DELIMITER;           		
                    }
                    Value_a=Value_a.trim();
                    Text t_value = new Text(name + DELIMITER + Value_a);
             context.write(t_key, t_value);
            	}
            }
           }
            
        }
    }

    public int run(String[] args)  throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    //    System.setProperty("hadoop.home.dir", "D:\\desktop\\hadoop-2.6.0");
        Configuration conf=new Configuration();
        String[] otherArgs=new String[]{"input","output1"}; 
        
        //每次运行前删除输出目录    
        Path outputPath = new Path(otherArgs[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        //添加分布式缓存文件 可以在map或reduce中直接通过users.txt链接访问对应缓存文件
  
        //需要加载的小表
      
         
	    	String fileIn=args[2];
	    	String keyIn=args[3];
	    	String keyCatch=args[1];
        	  conf.set("fileIn",fileIn);
    		  conf.set("keyIn",keyIn);
    		  conf.set("keyCatch",keyCatch);	  
    		  conf.set("d",args[0]);
        Job job = Job.getInstance(conf, "Broadcast_Join");
        job.setJarByClass(Broadcast_Join.class);
        job.setMapperClass(MyMappper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI(args[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]+"/" + args[0]));
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]+"/" + args[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        return job.waitForCompletion(true) ? 0 : 1;
//        long startTime = System.currentTimeMillis();
//        if (job.waitForCompletion(true))
//        {
//        	System.out.println("用时为"+(System.currentTimeMillis()-startTime));
//        	System.exit(1);
//        }
    }
}