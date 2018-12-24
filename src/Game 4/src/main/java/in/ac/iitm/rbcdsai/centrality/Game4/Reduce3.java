package in.ac.iitm.rbcdsai.centrality.Game4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// valueComparator provides functionality for comparison.
class valueComparator implements Comparator<Map.Entry<String, Integer>> {
	
    // compare compares two maps.
    //
    // @param m1 a map entry
    // @param m2 a map entry
    // @returns comparison result
	public int compare(Map.Entry<String, Integer> m1, Map.Entry<String, Integer> m2) {
		return m2.getValue().compareTo(m1.getValue());
	}
}


/**
 * Represents a Reduce3 class required for third stage of Reduce for Game4.
 * 
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Reduce3 extends Reducer<Text, Text, Text, FloatWritable> {

	private Text node = new Text();
	String neighborsList = null;
	float marginalContribution = 0;
	int weight = 0;
	int newWeight = 0;
	int maxInt = Integer.MAX_VALUE;
	int index = 0;
	float shapleyValue = 0;
	String neighbor = null;
	int bfsDistance = 0;
	float currentShapleyValue = 0;
	float currentSum = 0;
	float distanceFunction = 0;
	float indexFunction = 0;


    /**
     * Overrides the reduce method to provide functionality for Reduce.
     *
     * @param  key                 the key
     * @param  values              iterable of Text
     * @param  context             the context
     * @throws IOException         if IOException occured
     * @throws InteruptedException if InterruptedExecution occured
     */		
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
    	
    	float sum = 0;
    	Map<String,Integer> weightedNeighbors = new HashMap<String,Integer>();

    	for (Text neighbors : values) {
    		neighborsList = neighbors.toString();
	    	String[] tokens = neighborsList.split(":");
	    	
            for(int i = 0; i < tokens.length; i += 2) {
	    		weight = weightedNeighbors.containsKey(tokens[i]) ? weightedNeighbors.get(tokens[i]): maxInt;
	    		newWeight = Integer.parseInt(tokens[i + 1]);

	    		if (newWeight < weight) {
	    			weightedNeighbors.put(tokens[i], newWeight);
	    		}
	    	}
    	}

    	weightedNeighbors.remove(key.toString());
    	List<Map.Entry<String, Integer>> sortedMap = new ArrayList<Map.Entry<String,Integer>>(weightedNeighbors.entrySet());
    	index = sortedMap.size();

		if (index == 0) {
			context.write(key, new FloatWritable(1));
		}

		Collections.sort(sortedMap,new valueComparator());
		int prevDistance = -1;
		float prevShapleyValue = -1;

		for (Map.Entry<String, Integer> pair : sortedMap) {
			neighbor = pair.getKey();
			bfsDistance = pair.getValue();
			
            if (bfsDistance == prevDistance) {
				currentShapleyValue = prevShapleyValue;
            } else {
				distanceFunction = 1 / (1 + (float) (bfsDistance));
				currentShapleyValue = (distanceFunction / (float) (1 + index)) - sum;	
			}

			node.set(neighbor);
			context.write(node, new FloatWritable(currentShapleyValue));
			indexFunction = (float) index * (index + 1);
			currentSum = distanceFunction / indexFunction;
			sum = sum + currentSum;
			index--;
			prevDistance = bfsDistance;
			prevShapleyValue = currentShapleyValue;
		}

		shapleyValue = 1 - sum;
		context.write(key, new FloatWritable(shapleyValue));
	}
}
