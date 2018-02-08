package com.neu.is;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StateAnalysis {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HotelClusterBookingsAnalysis");
		job.setJarByClass(Downsample.class);

		job.setMapperClass(StateAnalysisMapper.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);

		job.setCombinerClass(StateAnalysisReducer.class);
		job.setReducerClass(StateAnalysisReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	
	

	/**
	 * @author shri
	 * 
	 *         Mapper class that uses Summarization Pattern to find the top
	 *         hotel clusters booked by users
	 * 
	 *         Using Text and LongWritable as KEYOUT, VALOUT for re-usability of
	 *         the code if hotel names are provided
	 *
	 */
	public static class StateAnalysisMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		static LongWritable one = new LongWritable(1);


		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			if (key.get() != 0) {
				// Parse value columns
				String[] data = value.toString().split(",");
				if (data.length == 24) {
					String isBooking = data[18];
					String hotelClusterId = data[23];

					// Check 'is_booking' = 1 for booked hotels 18
					if ("1".equals(isBooking)) {

						context.write(new Text(hotelClusterId), one);

					}
				}
			}
		}
	}

	/**
	 * @author shri
	 * 
	 *         Reducer class that uses Summarization Pattern to find the top
	 *         hotel clusters booked by users
	 *
	 */
	public static class StateAnalysisReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {


			long sum = 0;

			for (LongWritable val : values) {
				sum += val.get();
			}

			context.write(key, new LongWritable(sum));

		}

	}
	
}
