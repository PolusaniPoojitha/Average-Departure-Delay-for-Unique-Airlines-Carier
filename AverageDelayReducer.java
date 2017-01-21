package UIS.BigDataAnalytics.Assignment2;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class AverageDelayReducer extends Reducer<Text, AverageDelayMeanPair, Text, DoubleWritable>
{
	public void reduce(Text key, Iterable<AverageDelayMeanPair> values, Context context)
			throws IOException, InterruptedException
	{
		double sum=0;
		int count=0;
		for(AverageDelayMeanPair value: values)
		{
			sum+=value.getPartialSum().get();
			count+=value.getPartialCount().get();
		}
		
		context.write(key, new DoubleWritable(sum/count));
	}

}
