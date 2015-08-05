package cs455.hadoop.gigasort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GSReducer extends
		Reducer<LongWritable, NullWritable, LongWritable, Text> {

	private Text word = new Text();
	private int count=1;
	protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		
		if(count==1){
			context.write(key, word);
		}
		
		for(NullWritable val : values)
		{
			if(count%1000==0){
				context.write(key, word);
			}
			count++;
		}
		
	}

}
