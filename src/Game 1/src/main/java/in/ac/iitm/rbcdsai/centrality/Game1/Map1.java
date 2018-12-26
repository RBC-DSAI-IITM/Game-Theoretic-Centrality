package in.ac.iitm.rbcdsai.centrality.Game1;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Represents a Map1 class required for the first stage of Map for Game1.
 *
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Text nodeA = new Text();
  private Text nodeB = new Text();
  private Text edgeNode = new Text();
  long lineNumbers = 0;

  /**
   * Overrides the map method to provide functionality for Map.
   *
   * @param key the key
   * @param value the value
   * @param context the context
   * @throws IOException if an IOException occured
   * @throws InterruptedException if InterruptedException occured
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    lineNumbers += 1;
    int firstWord = 0;
    int secondWord = 0;

    try {
      StringTokenizer tokenizer = new StringTokenizer(line);
      int numberOfTokens = tokenizer.countTokens();

      if (numberOfTokens == 2) {
        nodeA.set(tokenizer.nextToken());
        nodeB.set(tokenizer.nextToken());
        boolean validInputLine = true;

        try {
          firstWord = Integer.parseInt(nodeA.toString());
          secondWord = Integer.parseInt(nodeB.toString());

          if (firstWord == secondWord) {
            String lineNumber = Long.toString(lineNumbers);
            System.err.println("The line " + lineNumber + " is a self loop.");
            validInputLine = false;
          }

        } catch (NumberFormatException f) {
          String lineNumber = Long.toString(lineNumbers);
          System.err.println("The line " + lineNumber + " has non-parsable integer.");
          validInputLine = false;
        }

        if (validInputLine) {
          edgeNode.set(nodeA);
          context.write(edgeNode, new IntWritable(secondWord));
          edgeNode.set(nodeB);
          context.write(edgeNode, new IntWritable(firstWord));
        }

      } else {
        String lineNumber = Long.toString(lineNumbers);
        String count = Integer.toString(numberOfTokens);
        System.err.println("The line " + lineNumber + " has " + count + " strings.");
      }

    } catch (NoSuchElementException e) {
      String lineNumber = Long.toString(lineNumbers);
      e.printStackTrace();
      System.err.println("The line " + lineNumber + " is empty.");
    }
  }
}
