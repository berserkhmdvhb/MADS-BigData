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
import org.apache.hadoop.mapreduce.Partitioner;

public class HadoopWordPairs_partitioned extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text pair = new Text();
		//private Text lastWord = new Text();

			
		final static Pattern pattern = Pattern.compile("(?!^[A-Z-_])(?>[a-z-_]*)|(?<=^| )\\d+(\\.\\d+)?(?=$| )|(?<=^| )\\.\\d+(?=$| )");
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] splitLine = value.toString().split(" ");
			Matcher matcher_1 = pattern.matcher("");
			Matcher matcher_2 = pattern.matcher("");
			int len = splitLine.length;
			int m = context.getConfiguration().getInt("m",1); //1 is default value
			for (int i = 0; i < len; i++) {
				matcher_1.reset(splitLine[i]);
				if(matcher_1.matches()) {
					for(int j = i+1; j < m+i+1; j++){
						if (j < len){
							matcher_2.reset(splitLine[j]);
							if (splitLine[j].length() > 0 && matcher_2.matches()) {
								pair.set(splitLine[i] + ":" + splitLine[j]);
								context.write(pair, one);
							}
						}
					}
				}	
			}
		}
	}

	
	
	public static class Partition extends Partitioner < Text, IntWritable >
	{
		
		final static Pattern pattern1 = Pattern.compile("([a-z-_]*)(:)([a-z-_]*)");
		final static Pattern pattern2 = Pattern.compile("([\\d.]*)(:)([\\d.]*)");
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
			  else if (matcher_2.matches()){
				  return 1;
			  }  
			  else {
				  return 2;
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

		Job job = Job.getInstance(conf, "HadoopWordPairs_partitioned");
		job.setJarByClass(HadoopWordPairs_partitioned.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(3);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs_partitioned(), args);
		System.exit(ret);
	}
}
