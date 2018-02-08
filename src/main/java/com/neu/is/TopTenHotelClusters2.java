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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author shri
 */
public class TopTenHotelClusters2 {

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Ton-n");
			job.setJarByClass(TopTenHotelClusters2.class);

			job.setMapperClass(TopTenClustersMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);

			job.setNumReduceTasks(0);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (IOException | InterruptedException | ClassNotFoundException ex) {
			System.out.println("Erorr Message" + ex.getMessage());
		}
	}

	public static class TopTenClustersMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private Map<String, Long> tm = new TreeMap<>();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] data = value.toString().split("	");
			String bookingCount = data[1];
			String hotelClusterId = data[0];

			tm.put(hotelClusterId, Long.parseLong(bookingCount));

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
				if (cnt != 10) {
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
