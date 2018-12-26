package in.ac.iitm.rbcdsai.centrality.Game1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Represents a Reduce1 class required for first stage of Reduce for Game1.
 *
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Reduce1 extends Reducer<Text, IntWritable, Text, FloatWritable> {

  private Text node = new Text();
  int degree = 0;
  int value = 0;
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
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    Set<Integer> neighbors = new HashSet<Integer>();

    for (IntWritable val : values) {
      value = val.get();
      neighbors.add(value);
    }

    degree = neighbors.size();
    marginalContribution = (float) 1 / (1 + degree);
    context.write(key, new FloatWritable(marginalContribution));

    for (Integer neighbor : neighbors) {
      node.set(neighbor.toString());
      context.write(node, new FloatWritable(marginalContribution));
    }
  }
}
