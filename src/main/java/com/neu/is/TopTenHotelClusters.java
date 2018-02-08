package com.neu.is;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.is.TopTenHotelClusters.CompositeKeyWritable;

/**
 *
 * @author shri
 */
public class TopTenHotelClusters {

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "SecondarySort");
			job.setJarByClass(TopTenHotelClusters.class);

			job.setMapperClass(TopTenClustersMapper.class);
			job.setMapOutputKeyClass(CompositeKeyWritable.class);
			job.setMapOutputValueClass(NullWritable.class);

			job.setPartitionerClass(Lab2Partitioner.class);
			job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

			job.setReducerClass(TopTenClustersReducer.class);
			job.setOutputKeyClass(CompositeKeyWritable.class);
			job.setOutputValueClass(NullWritable.class);
		
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (IOException | InterruptedException | ClassNotFoundException ex) {
			System.out.println("Erorr Message" + ex.getMessage());
		}
	}

	public static class TopTenClustersMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

		@Override
		public void map(LongWritable key, Text values, Context context) {
			if (values.toString().length() > 0) {
				try {

					if (key.get() != 0) {
						// Parse value columns
						String[] data = values.toString().split("	");
							String bookingCount = data[1];
							String hotelClusterId = data[0];


							CompositeKeyWritable cw  = new CompositeKeyWritable(hotelClusterId,Long.parseLong(bookingCount));
							
								context.write(cw, NullWritable.get());

						
					}

				} catch (IOException | InterruptedException ex) {
					Logger.getLogger(TopTenClustersMapper.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
		}

	}

	public static class TopTenClustersReducer
			extends Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {

		/**
		 *
		 * @param key
		 * @param values
		 * @param context
		 */
		@Override
		public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context context) {
			for (NullWritable val : values) {
				try {
					context.write(key, NullWritable.get());
				} catch (IOException | InterruptedException ex) {
					System.out.println("Error Message" + ex.getMessage());
				}
			}
		}
	}

	public static class NaturalKeyGroupingComparator extends WritableComparator {

		protected NaturalKeyGroupingComparator() {
			super(CompositeKeyWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKeyWritable cw1 = (CompositeKeyWritable) w1;
			CompositeKeyWritable cw2 = (CompositeKeyWritable) w2;

			return (cw1.getHotelClusterId().compareTo(cw2.getHotelClusterId()));
		}
	}

	public static class Lab2Partitioner extends Partitioner<CompositeKeyWritable, NullWritable> {

		@Override
		public int getPartition(CompositeKeyWritable key, NullWritable value, int numOfPartitions) {

			return (key.getHotelClusterId().hashCode() % numOfPartitions);
		}
	}

	public static class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

		private String hotelClusterId;
		private long bookingCount;

		public CompositeKeyWritable() {

		}

		public CompositeKeyWritable(String hotelClusterId, long bookingCount) {
			this.hotelClusterId = hotelClusterId;
			this.bookingCount = bookingCount;
		}

		public String getHotelClusterId() {
			return hotelClusterId;
		}

		public void setHotelClusterId(String hotelClusterId) {
			this.hotelClusterId = hotelClusterId;
		}

		public long getBookingCount() {
			return bookingCount;
		}

		public void setBookingCount(long bookingCount) {
			this.bookingCount = bookingCount;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, hotelClusterId);
			WritableUtils.writeVLong(out, bookingCount);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			hotelClusterId = WritableUtils.readString(in);
			bookingCount = WritableUtils.readVLong(in);
		}

		@Override
		public int compareTo(CompositeKeyWritable compositeKey) {
			int result = hotelClusterId.compareTo(compositeKey.hotelClusterId);
			if (result == 0) {
				result = Long.compare(bookingCount, compositeKey.getBookingCount());
			}
			return result;
		}

		@Override
		public String toString() {
			return (new StringBuilder().append(hotelClusterId).append("\t").append(bookingCount).toString());
		}
	}
}
