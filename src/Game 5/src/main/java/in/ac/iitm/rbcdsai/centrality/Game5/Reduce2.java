package in.ac.iitm.rbcdsai.centrality.Game5;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Represents a Reduce2 class required for second stage of Reduce for Game5.
 *
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Reduce2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  /**
   * Provides functionality for Reduce.
   *
   * @param key the key
   * @param values iterable of DoubleWritable
   * @param context the context
   * @throws IOException if IOException occured
   * @throws InteruptedException if InterruptedExecution occured
   */
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

    double centrality = 0;

    for (DoubleWritable val : values) {
      centrality += val.get();
    }
    context.write(key, new DoubleWritable(centrality));
  }
}
