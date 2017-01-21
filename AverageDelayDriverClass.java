package UIS.BigDataAnalytics.Assignment2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AverageDelayDriverClass extends Configured implements Tool  {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new Configuration(),new AverageDelayDriverClass(),args);
		System.exit(exitCode);
	}
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=new Job(getConf());
		job.setJarByClass(AverageDelayDriverClass.class);
		job.setJobName("AverageDelay");
		job.setMapperClass(AverageDelayMapper.class);
		job.setReducerClass(AverageDelayReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageDelayMeanPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		//job.setCombinerClass(AverageDelayCombiner.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}
}
