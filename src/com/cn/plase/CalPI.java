package com.cn.plase;

import java.io.IOException; 
import java.util.Random; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.DoubleWritable; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalPI { 
	public static class PiMapper 
		extends Mapper<Object, Text, Text, IntWritable>{ 
		
		private static Random rd = new Random(); 
		
		public void map(Object key, Text value, Context context ) 
					throws IOException, InterruptedException { int pointNum = Integer.parseInt(value.toString()); 
		for(int i = 0; i < pointNum; i++){ 
			// 取随机数 double 
			double x = rd.nextDouble(); 
			double y = rd.nextDouble(); 
			// 计算与(0.5,0.5)的距离，如果小于0.5就在单位圆里面 
			x -= 0.5; 
			y -= 0.5; 
			double distance = Math.sqrt(x*x + y*y); 
			
			IntWritable result = new IntWritable(0); 
			if (distance <= 0.5)
			{ 
				result = new IntWritable(1);
			} 
			context.write(value, result); 
				
		} 
	} 
}
	public static class PiReducer 
					extends Reducer<Text,IntWritable,Text,DoubleWritable> { 
			private DoubleWritable result = new DoubleWritable(); 
			public void reduce(Text key, Iterable<IntWritable> values, 
					Context context 
					) throws IOException, InterruptedException { 
				double pointNum = Double.parseDouble(key.toString()); 
				double sum = 0; 
				for (IntWritable val : values) {
					sum += val.get(); } 
				result.set(sum/pointNum*4); 
				context.write(key, result); 
				} 
			}
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration(); 
		conf.set("dfs.client.use.datanode.hostname", "true");
		Job job = Job.getInstance(conf,"calculate pi"); 
		job.setJarByClass(CalPI.class); 
		job.setMapperClass(PiMapper.class); 
		job.setReducerClass(PiReducer.class); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class); 
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
		} 
	}
