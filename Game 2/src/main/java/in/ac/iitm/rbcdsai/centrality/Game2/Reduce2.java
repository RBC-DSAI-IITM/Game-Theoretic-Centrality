package in.ac.iitm.rbcdsai.centrality.Game2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Represents a Reduce2 class required for second stage of Reduce for Game2.
 * 
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Reduce2 extends Reducer<Text, FloatWritable, Text, FloatWritable> {


    /**
     * Provides functionality for Reduce.
     *
     * @param  key                 the key
     * @param  values              iterable of FloatWritable
     * @param  context             the context
     * @throws IOException         if IOException occured
     * @throws InteruptedException if InterruptedExecution occured
     */
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
        throws IOException, InterruptedException {

		float centrality = 0;

	    for (FloatWritable val : values) {
	    	centrality += val.get();
	    }

		context.write(key,new FloatWritable(centrality));
	}
}
