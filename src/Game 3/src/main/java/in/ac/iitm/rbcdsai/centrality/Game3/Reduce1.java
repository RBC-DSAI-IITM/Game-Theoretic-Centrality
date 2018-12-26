package in.ac.iitm.rbcdsai.centrality.Game3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Represents a Reduce1 class required for first stage of Reduce for Game3.
 *
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Reduce1 extends Reducer<Text, Text, Text, Text> {

  private Text node = new Text();
  private Text neighbors = new Text();
  int hopDistance;
  int iterationCount;

  /** Types of bfsAlive. */
  public enum bfsAlive {
    bfsCount
  }

  /**
   * Overrides the setup method to provide functionality for Setup.
   *
   * @param context the context
   */
  @Override
  protected void setup(Context context) {
    Configuration conf1 = context.getConfiguration();
    hopDistance = conf1.getInt("HopDistance", 0);
    iterationCount = conf1.getInt("IterationCount", 0);
  }

  /**
   * Overrides the reduce method to provide functionality for Reduce.
   *
   * @param key the key
   * @param values iterable of IntWritable
   * @param context the context
   * @throws IOException if IOException occured
   * @throws InteruptedException if InterruptedExecution occured
   */
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    int weight = 0;
    String outerNeighbor = null;
    int outerWeight = 0;
    int innerWeight = 0;
    String neighborString = "";
    String strNeighbor = null;
    int newWeight = 0;

    Map<String, Integer> neighbors2weights = new HashMap<String, Integer>();

    for (Text neighbor : values) {
      weight = neighbors2weights.containsKey(neighbor) ? neighbors2weights.get(neighbor) : 0;
      strNeighbor = neighbor.toString();
      neighbors2weights.put(strNeighbor, weight + 1);
    }

    Set<Map.Entry<String, Integer>> pairs = neighbors2weights.entrySet();
    Map<Integer, ArrayList<String>> weight2neighbors = new HashMap<Integer, ArrayList<String>>();

    for (Map.Entry<String, Integer> outerPair : pairs) {
      outerNeighbor = outerPair.getKey();
      outerWeight = outerPair.getValue();
      neighbors.set(outerNeighbor + ":" + Integer.toString(outerWeight) + ":");
      context.write(key, neighbors);

      if (weight2neighbors.get(outerWeight) == null) {
        weight2neighbors.put(outerWeight, new ArrayList<String>());
      }

      weight2neighbors.get(outerWeight).add(outerNeighbor);
    }

    Map<Integer, String> weight2neighborString = new HashMap<Integer, String>();
    Set<Map.Entry<Integer, ArrayList<String>>> weightedPairs = weight2neighbors.entrySet();
    Set<Integer> weights = weight2neighbors.keySet();

    for (int NeighborWeight : weights) {
      neighborString = "";

      for (Map.Entry<Integer, ArrayList<String>> weightedPair : weightedPairs) {
        ArrayList<String> Neighbors = weightedPair.getValue();
        innerWeight = weightedPair.getKey();
        newWeight = NeighborWeight + innerWeight;

        if (newWeight <= hopDistance) {
          if (newWeight > iterationCount && newWeight < hopDistance) {
            context.getCounter(bfsAlive.bfsCount).increment(new Long(1));
          }

          for (String Neighbor : Neighbors) {
            neighborString = neighborString.concat(Neighbor);
            neighborString = neighborString.concat(":");
            neighborString = neighborString.concat(Integer.toString(newWeight));
            neighborString = neighborString.concat(":");
          }
        }
      }

      weight2neighborString.put(NeighborWeight, neighborString);
    }

    for (Map.Entry<String, Integer> outerPair : pairs) {
      outerNeighbor = outerPair.getKey();
      outerWeight = outerPair.getValue();
      node.set(outerNeighbor);
      String outputNeighbors = weight2neighborString.get(outerWeight);

      if (outputNeighbors != null) {
        neighbors.set(outputNeighbors);
        context.write(node, neighbors);
      }
    }
  }
}
