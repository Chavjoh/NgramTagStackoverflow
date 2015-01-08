package ch.hesso.master;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import ch.hesso.master.utils.StringToIntMapWritable;

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
	
	public static class StackTestMapper extends Mapper<LongWritable, StackoverflowPost, Text, StringToIntMapWritable> {
		
		private HashMap<String, StringToIntMapWritable> map;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			map = new HashMap<String, StringToIntMapWritable>();
		}

		@Override
		public void map(LongWritable key, StackoverflowPost value, Context context) throws java.io.IOException, InterruptedException {

			String[] tokens = new String[value.getTags().size()];
			value.getTags().toArray(tokens);
			
			for (int i = 0; i < tokens.length-1; i++) {
				StringToIntMapWritable stripes = map.get(tokens[i]);
				
				if (stripes == null) {
					stripes = new StringToIntMapWritable();
					map.put(tokens[i], stripes);
				}
				
				stripes.increment(tokens[i+1]);		
			}
			
			// TODO: Send data when memory we are out of memory
		}
				
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {	
			for (Entry<String, StringToIntMapWritable> entry : map.entrySet())
				context.write(new Text(entry.getKey()), entry.getValue());
			
			super.cleanup(context);
		}
	}

	public static class StackTestReducer extends Reducer<Text, StringToIntMapWritable, Text, StringToIntMapWritable> {

	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "StackTest");

		job.setMapperClass(StackTestMapper.class);
		job.setReducerClass(StackTestReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringToIntMapWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringToIntMapWritable.class);

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
		
		FileSystem.get(conf).delete(outputPath, true);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StackTest(args), args);
		System.exit(res);
	}
}
