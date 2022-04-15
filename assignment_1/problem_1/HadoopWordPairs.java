import java.io.IOException;
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

public class HadoopWordPairs extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text pair = new Text();
		//private Text lastWord = new Text();

			
		final static Pattern pattern = Pattern.compile("(?!^[A-Z-_])(?>[a-z-_]*)|(?<=^| )\\d+(\\.\\d+)?(?=$| )|(?<=^| )\\.\\d+(?=$| )");
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] splitLine = value.toString().split(" ");
			Matcher matcher = pattern.matcher("");

			int len = splitLine.length;
			Boolean[] matchesPattern = new Boolean[len];
			
			for(int i=0; i < len; i++){
				matcher.reset(splitLine[i]);
				matchesPattern[i] = matcher.matches();
			}
			Configuration conf = context.getConfiguration();
			int m = conf.getInt("m",1); //1 is default value

			for (int i = 0; i < len; i++) {
				if(splitLine[i].length() > 0 && matchesPattern[i]) {
					for(int j = i+1; j < m+i+1; j++){
						if (j < len){
							if (splitLine[j].length() > 0 && matchesPattern[j]) {
								pair.set(splitLine[i] + ":" + splitLine[j]);
								context.write(pair, one);
							}
						}
					}
				}	
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

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

		int m = Integer.parseInt(args[args.length -1]);
		Configuration conf = new Configuration();
		conf.setInt("m", m);

		Job job = Job.getInstance(conf, "HadoopWordPairs");
		job.setJarByClass(HadoopWordPairs.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		double start = System.nanoTime();
		int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs(), args);
		double stop = System.nanoTime();
		double diff_in_s = (stop - start) /  1e9;
		int secs = (int) diff_in_s % 60;
		int mins = (int) (diff_in_s / 60.0) % 60;
		System.out.println("Elapsed time: " + mins + " min " + secs + " s");
		System.exit(ret);
	}
}
