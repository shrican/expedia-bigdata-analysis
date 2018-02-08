package com.neu.is;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author shri
 *
 *	Random Sampling Filtering Pattern (Map only Job)
 *	
 *	MapReduce program to down sample the big data file 
 *
 *
 */
public class Downsample {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Downsample");
		job.setJarByClass(Downsample.class);

		job.setMapperClass(DownsamplingMapper.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	/**
	 * @author shri
	 *
	 *	Mapper to select random 10000 rows from the big data file for processing 
	 */
	public static class DownsamplingMapper extends Mapper<Object, Text, NullWritable, Text> {

		static int cnt = 0;

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {

			if (Math.random() < 0.5 && cnt < 10000) {
				cnt++;
				context.write(NullWritable.get(), value);
			}
		}
	}

}
