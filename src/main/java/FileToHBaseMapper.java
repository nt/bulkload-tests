import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;



public class FileToHBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		
		String[] vals = value.toString().split(",");
		String row = new StringBuilder(vals[0]).append(".").append(vals[1]).append(".").append(vals[2]).append(".").append(vals[3]).toString();
		
		Put p = new Put(row.getBytes());
		p.add("data".getBytes(), "d".getBytes(), Bytes.toBytes(Long.parseLong(vals[4])));
		
		context.write(new ImmutableBytesWritable(row.getBytes()), p);
		
	}
	
	public static void configureJob(Job job) throws IOException {
		job.setJarByClass(FileToHBaseMapper.class);
		job.setMapperClass(FileToHBaseMapper.class);
		
		TableMapReduceUtil.initTableReducerJob("table", null, job);
		job.setNumReduceTasks(0);
	}
	
}