package in.ac.iitm.rbcdsai.centrality.Game4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


/**
 * Represents the Main class.
 * 
 * @author M Vishnu Sankar
 * @version 1.0
 */
public class Main {

    /**
     * Performs the execution of the MapReduce task Game4.
     *
     * @param  args      requires an input file path as the first argument
     *                   and output file path as the second argument
     * @throws Exception if exception occured
     */
	public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: Game4 <InPath> <OutPath>");
            System.exit(2);
        }

		int iterationCount = 1;
		int hopDistance = 2;
		long counterValue = 0;

		Configuration conf1 = new Configuration();
		conf1.setInt("HopDistance", hopDistance);
		conf1.setInt("IterationCount", iterationCount);
		conf1.set("mapred.textoutputformat.separator", ":");
		Job job1 = new Job(conf1, "TwoHops");
		job1.setJarByClass(Main.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setNumReduceTasks(32);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + iterationCount));
		job1.waitForCompletion(true);
		Counters job1Counters = job1.getCounters();
		Counter reduce1Counter = job1Counters.findCounter(Reduce1.bfsAlive.bfsCount);
		counterValue = reduce1Counter.getValue();

		while (counterValue != 0) {
			Configuration conf2 = new Configuration();
			conf2.setInt("IterHopDistance", hopDistance);
			conf2.setInt("IterIterationCount", iterationCount+1);
			conf2.set("mapred.textoutputformat.separator", ":");
			Job job2 = new Job(conf2, "Iteration");
			job2.setJarByClass(Main.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			job2.setNumReduceTasks(32);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job2, new Path(args[1]+ iterationCount));
			iterationCount = iterationCount + 1;
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+ iterationCount));
			job2.waitForCompletion(true);
			Counters job2Counters = job2.getCounters();
			Counter reduce2Counter = job2Counters.findCounter(Reduce2.bfsAlive.bfsCount);
			counterValue = reduce2Counter.getValue();
		}

		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3, "Game4Phase3");
		job3.setJarByClass(Main.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapperClass(Map3.class);
		job3.setReducerClass(Reduce3.class);
		job3.setNumReduceTasks(32);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(args[1] + iterationCount));
		iterationCount = iterationCount + 1;
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + iterationCount));
		job3.waitForCompletion(true);

		Configuration conf4 = new Configuration();
		Job job4 = new Job(conf4, "Game4Phase4");
		job4.setJarByClass(Main.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(FloatWritable.class);
		job4.setMapperClass(Map4.class);
		job4.setReducerClass(Reduce4.class);
		job4.setNumReduceTasks(32);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job4, new Path(args[1] + iterationCount));
		iterationCount = iterationCount + 1;
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + iterationCount));
		job4.waitForCompletion(true);
	}
}
