package in.ac.iitm.rbcdsai.centrality.Game2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Represents the Main class.
 *
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Main {

  /**
   * Performs the execution of the MapReduce task Game2.
   *
   * @param args requires an input file path as the first argument and output file path as the
   *     second argument
   * @throws Exception if exception occured
   */
  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("Usage: Game2 <InPath> <OutPath>");
      System.exit(2);
    }

    int iterationCount = 1;

    Configuration conf1 = new Configuration();
    Job job1 = new Job(conf1, "Game2Phase1");
    job1.setJarByClass(Main.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setMapperClass(Map1.class);
    job1.setReducerClass(Reduce1.class);
    job1.setNumReduceTasks(32);
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1] + iterationCount));
    job1.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = new Job(conf2, "Game2Phase2");
    job2.setJarByClass(Main.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(FloatWritable.class);
    job2.setMapperClass(Map2.class);
    job2.setCombinerClass(Reduce2.class);
    job2.setReducerClass(Reduce2.class);
    job2.setNumReduceTasks(32);
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job2, new Path(args[1] + iterationCount));
    iterationCount += 1;
    FileOutputFormat.setOutputPath(job2, new Path(args[1] + iterationCount));
    job2.waitForCompletion(true);
  }
}
