package in.ac.iitm.rbcdsai.centrality.Game4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Represents a Map1 class required for the first stage of Map for Game4.
 * 
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Map1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text node = new Text();
	private Text neighbor = new Text();


    /**
     * Overrides the map method to provide functionality for Map.
     * 
     * @param  key                  the key
     * @param  value                the value
     * @param  context              the context
     * @throws IOException          if an IOException occured
     * @throws InterruptedException if InterruptedException occured
     */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		node.set(tokenizer.nextToken());
		neighbor.set(tokenizer.nextToken());
        context.write(node, neighbor);
        context.write(neighbor, node);
	}
}
