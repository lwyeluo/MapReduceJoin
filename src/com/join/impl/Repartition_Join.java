package com.join.impl;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Repartition_Join extends Configured implements Tool{
    public static final String DELIMITER = "\t"; // 字段分隔符

    static class MyMappper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

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
            //System.out.println(line);
            // 抛弃空记录
            if (line == null || line.trim().equals("")) return;

            String[] values = line.split(DELIMITER);
            // 处理user.txt数据
            if (filePath.contains(a)) {
                if (values.length < 2)  return;
                System.out.println(values[1]);
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
            // 处理login_logs.txt数据
            else if (filePath.contains(b)) {
                if (values.length < 2)  return;
       //System.out.println(values[2]);             
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


    public int run(String[] args)throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
   //     System.setProperty("hadoop.home.dir", "D:\\desktop\\hadoop-2.6.0");
        Configuration conf=new Configuration();     
        conf.set("a",args[0]);
		  conf.set("a1",args[1]);
		  conf.set("b",args[2]);
		  conf.set("b1",args[3]);
        String[] otherArgs=new String[]{"input","output2"}; 
        if (otherArgs.length!=2) {
            System.err.println("Usage:invertedindex<in><out>");
            System.exit(2);
        }
    //每次运行前删除输出目录    
        Path outputPath = new Path(otherArgs[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        
        Job job = Job.getInstance(conf, "Repartition_Join");
        job.setJarByClass(Repartition_Join.class);
        job.setMapperClass(MyMappper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]+"/" + args[0]));
	     FileInputFormat.addInputPath(job, new Path(otherArgs[0]+"/" + args[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
 
        long startTime = System.currentTimeMillis();
        return job.waitForCompletion(true) ? 0 : 1;
//        if (job.waitForCompletion(true))
//        {
//        	System.out.println("用时为"+(System.currentTimeMillis()-startTime));
//        	System.exit(1);
//        }
    }



}