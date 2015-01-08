package ch.hesso.master.count;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch.hesso.master.utils.StackoverflowPost;
import ch.hesso.master.utils.StackoverflowXMLInputFormat;
import ch.hesso.master.utils.StringToIntMapWritable;

public class TagsCount extends Configured implements Tool {
	
	private int numReducers;
	private Path inputPath;
	private Path outputPath;
	
	/**
	 * Stripes Constructor.
	 * 
	 * @param args
	 */
	public TagsCount(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: TagsCount <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		
		numReducers = Integer.parseInt(args[0]);
		inputPath = new Path(args[1]);
		outputPath = new Path(args[2]);
	}
	
	static class TagsCountMapper extends Mapper<LongWritable, StackoverflowPost, Text, IntWritable> {
		
		private HashMap<String, Integer> map;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			map = new HashMap<String, Integer>();
		}

		@Override
		protected void map(LongWritable key, StackoverflowPost value, Context context) throws IOException, InterruptedException {
			for (String word : value.getTags())
				map.put(word, (map.get(word) == null) ? 1 : map.get(word) + 1);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {	
			for(Entry<String, Integer> entry : map.entrySet())
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			
			super.cleanup(context);
		}
		
	}
	
	static class TagsCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable value : values)
				sum += value.get();
			
			context.write(key,new IntWritable(sum));
		}
	
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {	
			
			super.cleanup(context);
		}
	}


	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "TagsCount");

		job.setMapperClass(TagsCountMapper.class);
		job.setReducerClass(TagsCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		StackoverflowXMLInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(StackoverflowXMLInputFormat.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(numReducers);

		job.setJarByClass(TagsCount.class);
		
		FileSystem.get(conf).delete(outputPath, true);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TagsCount(args), args);
		System.exit(res);
	}
}
