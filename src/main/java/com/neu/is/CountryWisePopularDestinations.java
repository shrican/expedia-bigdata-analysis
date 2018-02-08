package com.neu.is;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.is.MonthWiseWeekendBookings.CustomWritable;
import com.neu.is.MonthWiseWeekendBookings.MonthWiseWeekendBookingsMapper;

public class CountryWisePopularDestinations {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CountryWisePopularDestinations");
		job.setJarByClass(PopularDestinationsAmongFamilies.class);

		job.setMapperClass(CountryWisePopularDestinationsMapper.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);

		//job.setCombinerClass(CountryWisePopularDestinationsReducer.class);
		job.setReducerClass(CountryWisePopularDestinationsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		// Configuration conf2 = new Configuration();
		// Job job2 = Job.getInstance(conf2, "Chaining");
		//
		// boolean complete = job.waitForCompletion(true);
		//
		// if (complete) {
		//
		// job2.setJarByClass(PopularDestinationsAmongFamilies.class);
		//
		// job2.setMapperClass(PopularDestinationsSortedMapper.class);
		// job2.setMapOutputKeyClass(Text.class);
		// job2.setMapOutputValueClass(Text.class);
		//
		// job2.setNumReduceTasks(0);
		// FileInputFormat.addInputPath(job2, new Path(args[2]));
		// FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		// System.exit(job2.waitForCompletion(true) ? 0 : 1);
		//
		// }

	}

	public static class CountryWisePopularDestinationsMapper extends Mapper<LongWritable, Text, IntWritable, CustomWritable> {

		private static final SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm");

		@Override
		public void map(LongWritable key, Text values, Context context) {
			if (values.toString().length() > 0) {
				try {

					if (key.get() != 0) {
						// Parse value columns
						String[] data = values.toString().split(",");

						if (data.length == 50) {

							int satBooking = Integer.parseInt(data[22]);

							int weekdayBookings = 0;
							int weekendBookings = 0;
							int totalBookings = 1;

							if (satBooking == 1) {

								weekendBookings = 1;

							} else {
								weekdayBookings = 1;
							}

							String timeStamp = data[1];
							Date date = new Date();

							date = format.parse(timeStamp);

							Calendar c = Calendar.getInstance();
							c.setTime(date);

							int month = c.get(Calendar.MONTH) + 1;

							CustomWritable cw = new CustomWritable(weekendBookings, weekdayBookings, totalBookings);

							context.write(new IntWritable(month), cw);
						}
					}

				} catch (IOException | InterruptedException ex) {
					Logger.getLogger(MonthWiseWeekendBookingsMapper.class.getName()).log(Level.SEVERE, null, ex);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	public static class CountryWisePopularDestinationsReducer
			extends Reducer<IntWritable, CustomWritable, IntWritable, CustomWritable> {

		/**
		 *
		 * @param key
		 * @param values
		 * @param context
		 * @throws InterruptedException
		 * @throws IOException
		 */
		@Override
		public void reduce(IntWritable key, Iterable<CustomWritable> values, Context context)
				throws IOException, InterruptedException {

			int weekendBookings = 0;
			int weekdayBookings = 0;
			int cnt = 0;

			for (CustomWritable val : values) {

				cnt++;
				weekdayBookings += val.getWeekdayBookings();
				weekendBookings += val.getWeekendBookings();

			}

			CustomWritable cw = new CustomWritable(weekendBookings, weekdayBookings, cnt);

			context.write(key, cw);
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

	public static class CustomWritable implements Writable {

		private int countryId;
		private int destinationId;
		private int totalBookings;

		public CustomWritable() {

		}

		public int getCountryId() {
			return countryId;
		}

		public void setCountryId(int countryId) {
			this.countryId = countryId;
		}

		public int getDestinationId() {
			return destinationId;
		}

		public void setDestinationId(int destinationId) {
			this.destinationId = destinationId;
		}

		public int getTotalBookings() {
			return totalBookings;
		}

		public void setTotalBookings(int totalBookings) {
			this.totalBookings = totalBookings;
		}

		public CustomWritable(int weekendBookings, int weekdayBookings, int totalBookings) {
			this.destinationId = weekdayBookings;
			this.countryId = weekendBookings;
			this.totalBookings = totalBookings;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeVInt(out, countryId);
			WritableUtils.writeVInt(out, destinationId);
			WritableUtils.writeVInt(out, totalBookings);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			countryId = WritableUtils.readVInt(in);
			destinationId = WritableUtils.readVInt(in);
			totalBookings = WritableUtils.readVInt(in);
		}

		@Override
		public String toString() {
			return (new StringBuilder().append(countryId).append("\t").append(destinationId).append("\t")
					.append(totalBookings).toString());
		}
	}

}
