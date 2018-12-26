package in.ac.iitm.rbcdsai.centrality.Game4;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Represents a Map4 class required for the fourth stage of Map for Game4.
 *
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Map4 extends Mapper<LongWritable, Text, Text, FloatWritable> {

  private Text node = new Text();
  float marginalContribution = 0;

  /**
   * Provides functionality for Map.
   *
   * @param key the key
   * @param value the value
   * @param context the context
   * @throws IOException if an IOException occured
   * @throws InterruptedException if InterruptedException occured
   */
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    StringTokenizer tokenizer = new StringTokenizer(line);
    node.set(tokenizer.nextToken());
    marginalContribution = Float.parseFloat(tokenizer.nextToken());
    context.write(node, new FloatWritable(marginalContribution));
  }
}
