package in.ac.iitm.rbcdsai.centrality.Game4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Represents a Map2 class required for the second stage of Map for Game4.
 * 
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Map2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text node = new Text();
	private Text neighbors = new Text();
	int spaceIndex = 0;
	String firstWord = null;
	String restWord = null;


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
		spaceIndex = line.indexOf(":");
		firstWord = line.substring(0,spaceIndex);
		restWord = line.substring(spaceIndex+1);
		node.set(firstWord);
		neighbors.set(restWord);
        context.write(node, neighbors);
	}
}
