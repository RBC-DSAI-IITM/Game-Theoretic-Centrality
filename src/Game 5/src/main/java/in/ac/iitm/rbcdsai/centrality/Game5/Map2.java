package in.ac.iitm.rbcdsai.centrality.Game5;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Represents a Map2 class required for the second stage of Map for Game5.
 * 
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Map2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text node = new Text();
    double centrality = 0; 


    /**
     * Provides functionality for Map.
     * 
     * @param  key                  the key
     * @param  value                the value
     * @param  context              the context
     * @throws IOException          if an IOException occured
     * @throws InterruptedException if InterruptedException occured
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
        String line = value.toString();
    	StringTokenizer tokenizer = new StringTokenizer(line);
    	node.set(tokenizer.nextToken());
    	centrality = Double.parseDouble(tokenizer.nextToken());
    	context.write(node, new DoubleWritable(centrality));
    }
}
