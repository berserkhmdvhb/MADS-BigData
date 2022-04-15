import java.io.IOException;

// regular expression to extract only the tokens we want
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Partitioner;

public class HadoopWordCount extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		final static Pattern pattern = Pattern.compile("(?!^[A-Z-_])(?>[a-z-_]*)|(?<=^| )\\d+(\\.\\d+)?(?=$| )|(?<=^| )\\.\\d+(?=$| )"); //include the correct pattern


		// this is the function we have to modify for (a)
		// only proper, lower-case words and
		// numbers with no more than one dot
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] splitLine = value.toString().split(" ");

			Matcher matcher = pattern.matcher("");

			for (String w : splitLine) {
				matcher.reset(w);
				
				// check if the word matches our desired pattern
				// if yes, emit a count of 1 for that word
				if (w.length() > 0 && matcher.matches()){
					word.set(w);
					context.write(word, one);
				}
				else continue;

			}
		}
	}
	
	public static class Partition extends Partitioner < Text, IntWritable >
	   {
		
		final static Pattern pattern1 = Pattern.compile("[a-z-_]*");
		final static Pattern pattern2 = Pattern.compile("(?<=^| )\\d+(\\.\\d+)?(?=$| )|(?<=^| )\\.\\d+(?=$| )");
	      @Override
	      public int getPartition(Text key, IntWritable value, int numReduceTasks)
	      {
			  Matcher matcher_1 = pattern1.matcher(key.toString());
			  Matcher matcher_2 = pattern2.matcher(key.toString());
			  //matcher_1.reset();
			  //matcher_2.reset();
			  //if(numReduceTasks == 0)
				//{
				    //return 0;
				//}
			  if(matcher_1.matches()){
					return 0;
				}
			  else if (matcher_2.matches())
					return 1;
			  return 2;
	      }
	   }
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		
		// this one tallies the counts and writes the output to two separate files
		// one for words, one for numbers
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			
			context.write(key, new IntWritable(sum));
		}

	}

	

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "HadoopWordCount");
		job.setJarByClass(HadoopWordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(2);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		double start = System.nanoTime();
		int ret = ToolRunner.run(new Configuration(), new HadoopWordCount(), args);
		double stop = System.nanoTime();
		double diff_in_s = (stop - start) /  1e9;
		int secs = (int) diff_in_s % 60;
		int mins = (int) (diff_in_s / 60.0) % 60;
		System.out.println("Elapsed time: " + mins + " min " + secs + " s");
		System.exit(ret);
	}
}
