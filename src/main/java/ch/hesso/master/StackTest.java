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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch.hesso.master.utils.StackoverflowPost;
import ch.hesso.master.utils.StackoverflowXMLInputFormat;
import ch.hesso.master.utils.StringToIntMapWritable;

public class StackTest extends Configured implements Tool {

	private final static Integer MAX_BUFFER_SIZE = 100000;
	
	private int numReducers;
	private Path inputPath;
	private String outputPath;
	private Integer ngramStart;
	private Integer ngramStop;
	
	/**
	 * Stripes Constructor.
	 * 
	 * @param args
	 */
	public StackTest(String[] args) {
		if (args.length != 5) {
			System.out.println("Usage: Ngram <ngram_start> <ngram_stop> <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		
		ngramStart = Integer.parseInt(args[0]);
		ngramStop = Integer.parseInt(args[1]);
		numReducers = Integer.parseInt(args[2]);
		inputPath = new Path(args[3]);
		outputPath = args[4];
		
		if (ngramStart < 0 || ngramStart > ngramStop) {
			System.out.println("Value error for <ngram_start> or <ngram_stop>");
		}
	}
	
	public static class StackTestMapper extends Mapper<LongWritable, StackoverflowPost, ArrayListWritable<Text>, StringToIntMapWritable> {
		
		private HashMap<ArrayListWritable<Text>, StringToIntMapWritable> map;
		private ArrayListWritable<Text> ngramKey;
		private int size;
		private int n;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			n = context.getConfiguration().getInt("N", 1);
			map = new HashMap<ArrayListWritable<Text>, StringToIntMapWritable>();
			size = 0;
		}

		@Override
		public void map(LongWritable key, StackoverflowPost value, Context context) throws java.io.IOException, InterruptedException {

			String[] tokens = new String[value.getTags().size()];
			value.getTags().toArray(tokens);
			
			for (int i = 0; i < tokens.length - n; i++) {
				ngramKey = new ArrayListWritable<Text>(); // No other way, need instantiation
				
				for (int j = 0; j < n; j++) {
					ngramKey.add(new Text(tokens[i+j]));
				}
				
				//System.out.println(ngramKey + " -> " + tokens[i+n]);
				
				StringToIntMapWritable stripes = map.get(ngramKey);
				
				if (stripes == null) {
					stripes = new StringToIntMapWritable();
					map.put(ngramKey, stripes);
				}
				
				stripes.increment(tokens[i+n]);
				size++;	
			}
			
			// Send data when we are out of memory
			if (size > MAX_BUFFER_SIZE) {
				sendMap(context);
			}
		}
				
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {	
			sendMap(context);
			super.cleanup(context);
		}
		
		private void sendMap(Context context) throws IOException, InterruptedException {
			for (Entry<ArrayListWritable<Text>, StringToIntMapWritable> entry : map.entrySet()) {
				context.write(entry.getKey(), entry.getValue());
			}
			
			map.clear();
			size = 0;
		}
	}

	public static class StackTestReducer extends Reducer<ArrayListWritable<Text>, StringToIntMapWritable, ArrayListWritable<Text>, StringToIntMapWritable> {

		private StringToIntMapWritable stripes;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			stripes = new StringToIntMapWritable();
		}
		
		@Override
		public void reduce(ArrayListWritable<Text> key, Iterable<StringToIntMapWritable> values, Context context) throws IOException, InterruptedException {
			stripes.clear();

			//System.out.println(key);
			
			for (StringToIntMapWritable value : values) {	
				stripes.sum(value);
			}
			
			context.write(key, stripes);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {	
			super.cleanup(context);
		}
		
	}

	public int run(String[] args) throws Exception {

		boolean result = false;
		
		for (int n = ngramStart; n <= ngramStop; n++) {
			result &= launchNgram(n);
		}
		
		return result ? 0 : 1;
	}
	
	private boolean launchNgram(int iteration) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = getConf();
		conf.setInt("N", iteration);
		
		Path output = new Path(outputPath + iteration);
		
		Job job = new Job(conf, "StackTest");

		job.setMapperClass(StackTestMapper.class);
		job.setReducerClass(StackTestReducer.class);

		job.setMapOutputKeyClass(ArrayListWritable.class);
		job.setMapOutputValueClass(StringToIntMapWritable.class);

		job.setOutputKeyClass(ArrayListWritable.class);
		job.setOutputValueClass(StringToIntMapWritable.class);

		StackoverflowXMLInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(StackoverflowXMLInputFormat.class);

		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(numReducers);

		job.setJarByClass(StackTest.class);
		
		FileSystem.get(conf).delete(output, true);
		
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StackTest(args), args);
		System.exit(res);
	}
}
