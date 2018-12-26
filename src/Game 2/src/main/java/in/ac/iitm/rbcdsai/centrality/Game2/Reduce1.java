package in.ac.iitm.rbcdsai.centrality.Game2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Represents a Reduce1 class required for first stage of Reduce for Game2.
 *
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Reduce1 extends Reducer<Text, IntWritable, Text, FloatWritable> {

  private Text node = new Text();
  int degree = 0;
  float threshold = 0;
  float marginalContribution = 0;
  boolean flag = false;
  Random randomNumber = new Random();

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

    int neigh = 0;
    Set<Integer> neighbors = new HashSet<Integer>();

    for (IntWritable val : values) {
      neigh = val.get();
      neighbors.add(neigh);
    }

    degree = neighbors.size();
    threshold = randomNumber.nextInt(degree + 1) + 1;
    marginalContribution = threshold / (1 + degree);
    context.write(key, new FloatWritable(marginalContribution));
    marginalContribution = (degree - threshold + 1) / (degree * (degree + 1));

    for (Integer neighbor : neighbors) {
      node.set(neighbor.toString());
      context.write(node, new FloatWritable(marginalContribution));
    }
  }
}
