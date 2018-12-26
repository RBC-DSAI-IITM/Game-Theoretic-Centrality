package in.ac.iitm.rbcdsai.centrality.Game3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Represents a Reduce2 class required for second stage of Reduce for Game3.
 *
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Reduce2 extends Reducer<Text, Text, Text, Text> {

  private Text node = new Text();
  private Text neighbors = new Text();
  String neighborsList = null;
  int hopDistance;
  int iterationCount;
  int maxInt = Integer.MAX_VALUE;

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
    Configuration conf2 = context.getConfiguration();
    hopDistance = conf2.getInt("IterHopDistance", 0);
    iterationCount = conf2.getInt("IterIterationCount", 0);
  }

  /**
   * Overrides the reduce method to provide functionality for Reduce.
   *
   * @param key the key
   * @param values iterable of Text
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
    String innerNeighbor = null;
    int innerWeight = 0;
    String neighborString = "";
    int newWeight = 0;

    Map<String, Integer> weightedNeighbors = new HashMap<String, Integer>();

    for (Text neighbor : values) {
      neighborsList = neighbor.toString();
      String[] tokens = neighborsList.split(":");

      for (int i = 0; i < tokens.length; i += 2) {
        weight = weightedNeighbors.containsKey(neighbor) ? weightedNeighbors.get(neighbor) : maxInt;
        newWeight = Integer.parseInt(tokens[i + 1]);

        if (newWeight < weight) {
          weightedNeighbors.put(tokens[i], newWeight);
        }
      }
    }

    Set<Map.Entry<String, Integer>> pairs = weightedNeighbors.entrySet();

    for (Map.Entry<String, Integer> outerPair : pairs) {
      outerNeighbor = outerPair.getKey();
      outerWeight = outerPair.getValue();
      neighbors.set(outerNeighbor + ":" + Integer.toString(outerWeight));
      context.write(key, neighbors);
      neighborString = "";

      for (Map.Entry<String, Integer> innerPair : pairs) {
        innerNeighbor = innerPair.getKey();
        innerWeight = innerPair.getValue();
        newWeight = outerWeight + innerWeight;

        if (newWeight <= hopDistance) {
          neighborString = neighborString.concat(innerNeighbor);
          neighborString = neighborString.concat(":");
          neighborString = neighborString.concat(Integer.toString(newWeight));
          neighborString = neighborString.concat(":");

          if (newWeight > iterationCount && newWeight < hopDistance) {
            context.getCounter(bfsAlive.bfsCount).increment(new Long(1));
          }
        }
      }

      if (!(neighborString.equals(""))) {
        node.set(outerNeighbor);
        neighbors.set(neighborString);
        context.write(node, neighbors);
      }
    }
  }
}
