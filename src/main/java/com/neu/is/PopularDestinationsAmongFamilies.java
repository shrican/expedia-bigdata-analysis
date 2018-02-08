package com.neu.is;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PopularDestinationsAmongFamilies {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PopularDestinationsAmongFamilies");
		job.setJarByClass(PopularDestinationsAmongFamilies.class);

		job.setMapperClass(PopularDestinationsMapper.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);

		job.setCombinerClass(PopularDestinationsReducer.class);
		job.setReducerClass(PopularDestinationsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Chaining");

		boolean complete = job.waitForCompletion(true);

		if (complete) {

			job2.setJarByClass(PopularDestinationsAmongFamilies.class);

			job2.setMapperClass(PopularDestinationsSortedMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setNumReduceTasks(0);
			FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);

		}

	}

	/**
	 * @author shri
	 * 
	 *
	 */
	public static class PopularDestinationsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		static LongWritable one = new LongWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			if (key.get() != 0) {
				// Parse value columns
				String[] data = value.toString().split(",");
				if (data.length == 24) {

					String isBooking = data[18];
					String destinationId = data[16];

					int noOfChildren = Integer.parseInt(data[14]);

					// Check 'is_booking' = 1 for booked hotels 18
					if ("1".equals(isBooking) && noOfChildren > 0) {

						context.write(new Text(destinationId), one);

					}
				}
			}
		}
	}

	/**
	 * @author shri
	 * 
	 *
	 */
	public static class PopularDestinationsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

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

	/**
	 * @author shri
	 * 
	 *
	 */
	public static class PopularDestinationsSortedMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private Map<String, Long> tm = new TreeMap<>();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] data = value.toString().split("	");
			String bookingCount = data[1];
			String destinationId = data[0];

			tm.put(destinationId, Long.parseLong(bookingCount));

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			Map sortedMap = sortByValues(tm);
			Set set = sortedMap.entrySet();

			// Get an iterator
			Iterator i = set.iterator();

			int cnt = 0;
			// Display elements
			while (i.hasNext()) {
				cnt++;
				if (cnt != 15) {
					Map.Entry me = (Map.Entry) i.next();

					Text key = new Text(me.getKey().toString());

					LongWritable value = new LongWritable(Long.parseLong(me.getValue().toString()));

					context.write(key, value);
				} else {
					break;
				}

			}
		}

	}

	public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		// LinkedHashMap will keep the keys in the order they are inserted
		// which is currently sorted on natural ordering
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

}
