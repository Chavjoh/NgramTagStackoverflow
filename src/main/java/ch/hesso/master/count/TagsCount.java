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
import ch.hesso.master.utils.Utils;

public class TagsCount extends Configured implements Tool {
	
	private int numReducers;
	private Path inputPath;
	private Path outputPath;
	private Path outputPathOrdered;
	
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
		String output = args[2];

		
		outputPath = new Path(output);
		outputPathOrdered = new Path(output + "-ordered");
	}
	
	static class TagsCountMapper extends Mapper<LongWritable, StackoverflowPost, Text, IntWritable> {
		
		private HashMap<String, Integer> map;
		private final Text TEXT = new Text();
		private final IntWritable INT = new IntWritable();

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
			
			for(Entry<String, Integer> entry : map.entrySet()){
				TEXT.set(entry.getKey());
				INT.set(entry.getValue());
				context.write(TEXT, INT);
			}
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
	
	static class TagsCountOrderMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		private IntWritable intValue;
		private Text textValue;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			textValue = new Text();
			intValue = new IntWritable();
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineValue = Utils.words(value.toString());
			textValue.set(lineValue[0]);
			intValue.set(Integer.parseInt(lineValue[1]));
			context.write(intValue, textValue);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
		
	}
	
	static class TagsCountOrderReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text text:values) {
				context.write(text, key);
			}
		}
		
	}

	public int run(String[] args) throws Exception {
		boolean result = false;
		
		result &= launchTagsCount();
		result &= launchTagsCountOrder();
		
		return (result) ? 0 : 1;
	}
	
	private boolean launchTagsCount() throws IOException, ClassNotFoundException, InterruptedException {
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
		
		return job.waitForCompletion(true);
	}
	
	private boolean launchTagsCountOrder() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = new Job(conf, "TagsCountOrder");

		job.setMapperClass(TagsCountOrderMapper.class);
		job.setReducerClass(TagsCountOrderReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		StackoverflowXMLInputFormat.addInputPath(job, outputPath);

		FileOutputFormat.setOutputPath(job, outputPathOrdered);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(numReducers);

		job.setJarByClass(TagsCount.class);
		
		FileSystem.get(conf).delete(outputPathOrdered, true);
		
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TagsCount(args), args);
		System.exit(res);
	}
}
