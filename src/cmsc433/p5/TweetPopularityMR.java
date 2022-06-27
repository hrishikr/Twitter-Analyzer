package cmsc433.p5;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Map reduce which takes in a CSV file with tweets as input and output
 * key/value pairs.</br>
 * </br>
 * The key for the map reduce depends on the specified {@link TrendingParameter}
 * , <code>trendingOn</code> passed to
 * {@link #score(Job, String, String, TrendingParameter)}).
 */
public class TweetPopularityMR {

	// For your convenience...
	public static final int          TWEET_SCORE   = 1;
	public static final int          RETWEET_SCORE = 3;
	public static final int          MENTION_SCORE = 1;
	public static final int			 PAIR_SCORE = 1;

	// Is either USER, TWEET, HASHTAG, or HASHTAG_PAIR. Set for you before call to map()
	private static TrendingParameter trendingOn;

	public static class TweetMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Converts the CSV line into a tweet object
			Tweet tweet = Tweet.createTweet(value.toString());

			// TODO: Your code goes here
			IntWritable tweetScore = new IntWritable(TWEET_SCORE);
			IntWritable retweetScore = new IntWritable(RETWEET_SCORE);
			IntWritable mentionScore = new IntWritable(MENTION_SCORE);
			IntWritable pairScore = new IntWritable(PAIR_SCORE);
			
			if (trendingOn == TrendingParameter.USER) {
				// Base case
				context.write(new Text(tweet.getUserScreenName()), tweetScore);
				
				// If tweet was retweet of user
				if (tweet.wasRetweetOfUser()) {
					context.write(new Text(tweet.getRetweetedUser()), retweetScore);
				}
				
				// Update score for mentioned users
				for (String mentioned : tweet.getMentionedUsers()) {
					context.write(new Text(mentioned), mentionScore);
				}
				
			} else if (trendingOn == TrendingParameter.TWEET) {
				// Base case
				context.write(new Text(tweet.getId().toString()), tweetScore);
				
				// If tweet was retweet of tweet
				if (tweet.wasRetweetOfTweet()) {
					context.write(new Text(tweet.getRetweetedTweet().toString()), retweetScore);
				}
				
			} else if (trendingOn == TrendingParameter.HASHTAG) {
				for (String hashtag : tweet.getHashtags()) {
					context.write(new Text(hashtag), tweetScore);
				}
				
			} else if (trendingOn == TrendingParameter.HASHTAG_PAIR) {
				List<String> hashtags = tweet.getHashtags();
				
				// Get them in reverse sorted order to get desired output formatting for hashtag pairs
				Collections.sort(hashtags, Collections.reverseOrder());
				for (int i = 0; i < hashtags.size(); i++) {
					for (int j = i + 1; j < hashtags.size(); j++) {
						context.write(new Text("(" + hashtags.get(i) + "," + hashtags.get(j) + ")"), pairScore);
					}
				}
			} 

		}
	}

	public static class PopularityReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// TODO: Your code goes here
			int score = 0;
			for(IntWritable value:values) {
				score += value.get();
			}
			System.out.println(key.toString() + " " + score);
			context.write(key, new IntWritable(score));
		}
	}

	/**
	 * Method which performs a map reduce on a specified input CSV file and
	 * outputs the scored tweets, users, or hashtags.</br>
	 * </br>
	 * 
	 * @param job
	 * @param input
	 *          The CSV file containing tweets
	 * @param output
	 *          The output file with the scores
	 * @param trendingOn
	 *          The parameter on which to score
	 * @return true if the map reduce was successful, false otherwise.
	 * @throws Exception
	 */
	public static boolean score(Job job, String input, String output,
			TrendingParameter trendingOn) throws Exception {

		TweetPopularityMR.trendingOn = trendingOn;

		job.setJarByClass(TweetPopularityMR.class);

		// TODO: Set up map-reduce...
		
		// Specify data type of output key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Specify names of Mapper and Reducer Class
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(PopularityReducer.class);

		// Specify formats of the data type of Input and output
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);


		// End

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}
}
