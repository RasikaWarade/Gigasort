package cs455.hadoop.gigasort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class GSPartitioner extends Partitioner<LongWritable, NullWritable> {

	@Override
	public int getPartition(LongWritable key, NullWritable value,
			int numofReduceTasks) {
		// TODO Auto-generated method stub
		
		// Partition based on the number of reducers
		Long zero=new Long("0");
		Long n = (long) (Math.pow(2, 63) - 1);
		Long range = n / numofReduceTasks;
		Long KY=key.get();
		
		//The loop partitions the data for the one less the number of reducers
		for(int l=1;l<numofReduceTasks;l++){

			//System.out.println("Range ["+(l-1)+"]:"+ range*(l-1)+" to "+range*l);
			if(KY.compareTo(range*(l-1))>=0 && KY.compareTo(range*l)<0){


				return (l-1);
			}
			
		}
		//if not found in any reducers above, it belongs to the last reducer
		return (numofReduceTasks-1);

	}

}
