package in.ac.iitm.rbcdsai.centrality.Game3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Represents a Reduce3 class required for third stage of Reduce for Game3.
 *
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Reduce3 extends Reducer<Text, Text, Text, FloatWritable> {

  private Text node = new Text();
  int extDegree = 0;
  String neighborsList = null;
  float marginalContribution = 0;

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

    Set<String> extNeighbors = new HashSet<String>();

    for (Text val : values) {
      neighborsList = val.toString();
      String[] tokens = neighborsList.split(":");

      for (int i = 0; i < tokens.length; i += 2) {
        extNeighbors.add(tokens[i]);
      }
    }
    extNeighbors.remove(key.toString());
    extDegree = extNeighbors.size();
    marginalContribution = (float) 1 / (1 + extDegree);
    context.write(key, new FloatWritable(marginalContribution));

    for (String neighbor : extNeighbors) {
      node.set(neighbor);
      context.write(node, new FloatWritable(marginalContribution));
    }
  }
}
