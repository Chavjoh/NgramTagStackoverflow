package ch.hesso.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch.hesso.master.utils.StackoverflowPost;
import ch.hesso.master.utils.StackoverflowXMLInputFormat;

public class StackTest extends Configured implements Tool {

	private int numReducers;
	private Path inputPath;
	private Path outputPath;
	
	/**
	 * Stripes Constructor.
	 * 
	 * @param args
	 */
	public StackTest(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: Ngram <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		
		numReducers = Integer.parseInt(args[0]);
		inputPath = new Path(args[1]);
		outputPath = new Path(args[2]);
	}
	
	public static class StackTestMapper extends Mapper<LongWritable, StackoverflowPost, LongWritable, StackoverflowPost> {
		
	}

	public static class StackTestReducer extends Reducer<LongWritable, StackoverflowPost, LongWritable, StackoverflowPost> {

	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "StackTest");

		job.setMapperClass(StackTestMapper.class);
		job.setReducerClass(StackTestReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(StackoverflowPost.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(StackoverflowPost.class);

		StackoverflowXMLInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(StackoverflowXMLInputFormat.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		
		boolean useTextOutputFormat = !false;
		if (useTextOutputFormat) {
			job.setOutputFormatClass(TextOutputFormat.class);
		}
		else {
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		}

		job.setNumReduceTasks(numReducers);

		job.setJarByClass(StackTest.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StackTest(args), args);
		System.exit(res);
	}
}
