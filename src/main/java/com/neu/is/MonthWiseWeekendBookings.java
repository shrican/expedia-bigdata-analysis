package com.neu.is;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonthWiseWeekendBookings {

	public static void main(String[] args) {
		try {
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "MonthWiseWeekendBookings");
			job.setJarByClass(TopTenHotelClusters.class);

			job.setMapperClass(MonthWiseWeekendBookingsMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(CustomWritable.class);

			job.setReducerClass(MonthWiseWeekendBookingsReducer.class);
			job.setOutputKeyClass(CustomWritable.class);
			job.setOutputValueClass(NullWritable.class);

//			job.setNumReduceTasks(0);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (IOException | InterruptedException | ClassNotFoundException ex) {
			System.out.println("Erorr Message" + ex.getMessage());
		}
	}

	public static class MonthWiseWeekendBookingsMapper extends Mapper<LongWritable, Text, IntWritable, CustomWritable> {

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

	public static class MonthWiseWeekendBookingsReducer
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

	public static class CustomWritable implements Writable {

		private int weekendBookings;
		private int weekdayBookings;
		private int totalBookings;

		public CustomWritable() {

		}

		public int getWeekendBookings() {
			return weekendBookings;
		}

		public void setWeekendBookings(int weekendBookings) {
			this.weekendBookings = weekendBookings;
		}

		public int getWeekdayBookings() {
			return weekdayBookings;
		}

		public void setWeekdayBookings(int weekdayBookings) {
			this.weekdayBookings = weekdayBookings;
		}

		public int getTotalBookings() {
			return totalBookings;
		}

		public void setTotalBookings(int totalBookings) {
			this.totalBookings = totalBookings;
		}

		public CustomWritable(int weekendBookings, int weekdayBookings, int totalBookings) {
			this.weekdayBookings = weekdayBookings;
			this.weekendBookings = weekendBookings;
			this.totalBookings = totalBookings;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeVInt(out, weekendBookings);
			WritableUtils.writeVInt(out, weekdayBookings);
			WritableUtils.writeVInt(out, totalBookings);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			weekendBookings = WritableUtils.readVInt(in);
			weekdayBookings = WritableUtils.readVInt(in);
			totalBookings = WritableUtils.readVInt(in);
		}

		@Override
		public String toString() {
			return (new StringBuilder().append(weekendBookings).append("\t").append(weekdayBookings).append("\t")
					.append(totalBookings).toString());
		}
	}

}
