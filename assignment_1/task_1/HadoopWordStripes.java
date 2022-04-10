import java.io.IOException;
import java.util.Iterator;

// regular expression to extract only the tokens we want
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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

public class HadoopWordStripes extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
		private final static IntWritable one = new IntWritable(1);

		final static Pattern pattern = Pattern.compile("(?!^[A-Z-_])(?>[a-z-_]*)|(?<=^| )\\d+(\\.\\d+)?(?=$| )|(?<=^| )\\.\\d+(?=$| )"); //include the correct pattern


		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] splitLine = value.toString().split(" ");
			Matcher matcher = pattern.matcher(" "); // first word

			Boolean[] matchesPattern = new Boolean[splitLine.length];

			int m = 2; // to test this

			for(int i=0; i < matchesPattern.length; i++){
				matcher.reset(splitLine[i]);
				matchesPattern[i] = matcher.matches();
			}

			for (int i = 0; i < splitLine.length; i++) {
				MapWritable map = new MapWritable();
				String w;
				
				// skip if the current token does not fit the pattern
				if (!matchesPattern[i]) continue;

				for(int j=1; j < m+1; j++){

					if (i-j >= 0 && matchesPattern[i-j]) {
						w = splitLine[i - j];
						stripe(w, map);
					}

					if (i+j < splitLine.length && matchesPattern[i+j]) {
						w = splitLine[i + j];
						stripe(w, map);
					}

				}
				
				context.write(new Text(splitLine[i]), map);
			}
		}

		public static void stripe(String w, MapWritable map) {
			LongWritable count = new LongWritable(0);

			if (map.containsKey(new Text(w))) {
				count = (LongWritable) map.get(new Text(w));
				map.remove(new Text(w));
			}

			count = new LongWritable(count.get() + one.get());
			map.put(new Text(w), count);
		}

	}

	public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {

		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			MapWritable stripe = new MapWritable();

			for (MapWritable localStripe : values) {
				Iterator entries = localStripe.entrySet().iterator();

				while (entries.hasNext()) {
					java.util.Map.Entry thisEntry = (java.util.Map.Entry) entries.next();
					Text keyNeighbour = (Text) thisEntry.getKey();
					LongWritable value = (LongWritable) thisEntry.getValue();
					globalStripe(keyNeighbour, value, stripe);
				}
			}

			context.write(key, stripe);
		}

		public static void globalStripe(Text key, LongWritable value, MapWritable map) {
			LongWritable sum = new LongWritable(0);

			if (map.containsKey(key)) {
				sum = (LongWritable) map.get(key);
				map.remove(key);
			}

			sum = new LongWritable(sum.get() + value.get());
			map.put(key, sum);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "HadoopWordStripes");
		job.setJarByClass(HadoopWordStripes.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

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
		int ret = ToolRunner.run(new Configuration(), new HadoopWordStripes(), args);
		System.exit(ret);
	}
}
