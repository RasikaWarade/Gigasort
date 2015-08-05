package cs455.hadoop.gigasort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GSMap extends
		Mapper<LongWritable, Text, LongWritable, NullWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	NullWritable nul = NullWritable.get();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// MapReduce has automatic sorting by keys after the map phase.
		// So arrange the numbers in key value pair
		// So to sort the input long numbers, make it key
		String line = value.toString();
		LongWritable num = new LongWritable();
		num.set(Long.parseLong(line));
		context.write(num, nul);

	}
}
