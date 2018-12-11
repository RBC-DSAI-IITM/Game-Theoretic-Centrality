package in.ac.iitm.rbcdsai.centrality.Game5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.special.Erf;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Represents a Reduce1 class required for first stage of Reduce for Game5.
 * 
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Reduce1 extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	int degree = 0;
	private Text node = new Text();
	int alpha = 0;
	int beta = 0;
	Random randomNumber = new Random();


    /**
     * Overrides the reduce method to provide functionality for Reduce.
     *
     * @param  key                 the key
     * @param  values              iterable of IntWritable
     * @param  context             the context
     * @throws IOException         if IOException occured
     * @throws InteruptedException if InterruptedExecution occured
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
	    
        int neigh = 0;
	    int weight = 0;
	    int count = 0;   
    	Map<Integer,Integer>  weightedNeighbors= new HashMap<Integer,Integer>(); 
    	
        for (IntWritable val : values) {
	    	neigh = val.get();
	    	count = weightedNeighbors.containsKey(neigh) ? weightedNeighbors.get(neigh): 0;
	    	weightedNeighbors.put(neigh, count+1);
	    }

    	List<Integer> neighbors = new ArrayList<Integer>(); 

    	for (Map.Entry<Integer, Integer> pairs : weightedNeighbors.entrySet()) {
    		neigh = pairs.getKey();
    		weight = pairs.getValue();
    		neighbors.add(neigh);
    		alpha += weight;
    		beta += (weight*weight);
    	}

    	degree = neighbors.size();
    	int threshold = randomNumber.nextInt(alpha + 1) + 1;
    	double shapleyValue = this.node(threshold);
    	node.set(key);
    	context.write(node, new DoubleWritable(shapleyValue));

    	for (int neighbor:neighbors) {
    		weight = weightedNeighbors.get(neighbor);
    		shapleyValue = this.neighbor(threshold,weight);
    		node.set(Integer.toString(neighbor));
    		context.write(node, new DoubleWritable(shapleyValue));
    	}

    	alpha = 0;
    	beta = 0;
    }


    /**
     * node returns the Shapley value based on a threshold.
     *
     * @param   threshold           the threshold
     * @returns shapleyValue        the Shapley value
     */
    public double node(int threshold) {

    	double mean = 0;
    	double variance = 0;
    	double standardDeviation = 0;
    	double probability = 0;
    	double numerator = 0;
    	double denominator = 0;
    	double term1 = 0;
    	double term2 = 0;
    	double shapleyValue = 0;

    	for (int m = 0; m <= degree; m++) {
    		mean = (m / (double)degree) * alpha;
    		numerator = m * (degree - m);
    		denominator = degree * (degree - 1);
    		
            if (numerator == 0.0) {
    			term1 = 0;
    		} else {
    			term1 = numerator / denominator;
    		}

    		term2 = beta - (alpha * alpha / (double)degree);
    		variance = term1 * term2;
    		
            if (variance == 0) {
    			if (mean < threshold) {
    				probability += 1;
    			}	
    		} else {
	    		standardDeviation = Math.sqrt(variance);
	    		term1 = (threshold - mean) / (Math.sqrt(2) * standardDeviation);
    			probability += 0.5 * Erf.erf(term1);
    		}
    		shapleyValue += probability / (1+degree);
    	}
    	return shapleyValue;
    }
    

    /**
     * neighbor returns the Shapley value based on a threshold and a weight.
     *
     * @param   threshold    the threshold
     * @param   weight       the weight
     * @returns shapleyValue the Shapley value
     */
    public double neighbor(int threshold,int weight){

    	double mean = 0;
    	double variance = 0;
    	double standardDeviation = 0;
    	double probability = 0;
    	double numerator = 0;
    	double denominator = 0;
    	double term1 = 0;
    	double term2 = 0;
    	double term3 = 0;
    	double shapleyValue = 0;

    	for (int m = 0; m <= degree - 1; m++) {
    		if (m == 0) {
    			term1 = 0;
    		} else {
    			term1 = m / (double)(degree - 1);
    		}

    		term2 = alpha - weight;
    		mean = term1 * term2;
    		numerator = m * (degree - 1 - m);
    		denominator = (degree - 1) * (degree - 2);
    		
            if (numerator == 0) {
    			term1 = 0;
    		} else {
    			term1 = numerator / denominator;
    		}

    		if (degree == 1) {
    			term3 = 0;
    		} else {
    			term3 = (alpha - weight) * (alpha - weight) / (double)(degree - 1);
    		}

    		term2 = beta - (weight * weight) - term3;
    		variance = term1 * term2;
    		
            if (variance == 0) {
    			if (mean < threshold) {
    				probability += 1;
    			}
    		} else {
    			standardDeviation = Math.sqrt(variance);
    			term1 = (threshold - mean) / (Math.sqrt(2) * standardDeviation);
	    		term2 = (threshold - weight - mean) / (Math.sqrt(2) * standardDeviation);
	    		probability += 0.5 * (Erf.erf(term1) - Erf.erf(term2));
    		}
    		shapleyValue += probability * (degree-m) / ((degree) * (1 + degree));
    	}
    	return shapleyValue;
    }
}
