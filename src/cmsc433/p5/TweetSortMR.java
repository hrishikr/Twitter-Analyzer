package cmsc433.p5;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cmsc433.p5.TweetPopularityMR.PopularityReducer;
import cmsc433.p5.TweetPopularityMR.TweetMapper;

/**
 * Map reduce which sorts the output of {@link TweetPopularityMR}.
 * The input will either be in the form of: </br>
 * 
 * <code></br>
 * &nbsp;(screen_name,  score)</br>
 * &nbsp;(hashtag, score)</br>
 * &nbsp;(tweet_id, score)</br></br>
 * </code>
 * 
 * The output will be in the same form, but with results sorted on the score.
 * 
 */
public class TweetSortMR {

	/**
	 * Minimum <code>int</code> value for a pair to be included in the output.
	 * Pairs with an <code>int</code> less than this value are omitted.
	 */
	private static int CUTOFF = 10;

	public static class SwapMapper
	extends Mapper<Object, Text, IntWritable, Text> {

		String      id;
		int         score;

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split("\t");
			id = columns[0];
			score = Integer.valueOf(columns[1]);

			// TODO: Your code goes here
			if (score >= CUTOFF) {
				// We swap the key/pair order here. This is done because mapreduce output 
				// sorts by key by default. I could have used secondary sort, but 
				// that's a bit challenging :(
				// Since we want the result in descending order, we just flip the sign
				// of the score. We can reverse it while reducing.
				context.write(new IntWritable(score * -1), new Text(id));
			}
		}
	}

	public static class SwapReducer
	extends Reducer<IntWritable, Text, Text, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// TODO: Your code goes here
			for (Text i : values) {
				// Flip the sign back and change of the key/value pair to 
				// get the desired format of output
				context.write(i, new IntWritable(key.get() * -1) );
			}

		}
	}


	/**
	 * This method performs value-based sorting on the given input by configuring
	 * the job as appropriate and using Hadoop.
	 * 
	 * @param job
	 *          Job created for this function
	 * @param input
	 *          String representing location of input directory
	 * @param output
	 *          String representing location of output directory
	 * @return True if successful, false otherwise
	 * @throws Exception
	 */
	public static boolean sort(Job job, String input, String output, int cutoff)
			throws Exception {

		CUTOFF = cutoff;

		job.setJarByClass(TweetSortMR.class);

		// TODO: Set up map-reduce...
		
		// Specify data type of output key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Specify names of Mapper and Reducer Class
		job.setMapperClass(SwapMapper.class);
		job.setReducerClass(SwapReducer.class);

		// Specify formats of the data type of Input and output
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		// End

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}

}
