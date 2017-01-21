package UIS.BigDataAnalytics.Assignment2;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
public class AverageDelayMapper extends Mapper<LongWritable,Text,Text,AverageDelayMeanPair>{

	public  void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
	{
	   String []columns=value.toString().split(",");
		   try{
			   String UniqueCarrier=columns[8];
			   double delay=Double.parseDouble(columns[15]);
			   int count=1;
			   context.write(new Text(UniqueCarrier), new AverageDelayMeanPair(delay, count));
	        }
		   catch(Exception ex)
		   {
			   
		   }
	}
}
