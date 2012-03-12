import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * http://hbase.apache.org/book/mapreduce.example.html
 * @author nicolas
 *
 */
public class MRToHBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		
		String[] vals = value.toString().split(",");
		String row = new StringBuilder(vals[0]).append(".").append(vals[1]).append(".").append(vals[2]).append(".").append(vals[3]).toString();
		
//		Put p = new Put(row.getBytes());
//		p.add("data".getBytes(), "d".getBytes(), Bytes.toBytes(Long.parseLong(vals[4])));
		
		KeyValue kv =
			new KeyValue(row.getBytes(), "data".getBytes(), "d".getBytes(), Bytes.toBytes(Long.parseLong(vals[4])));
		
		context.write(new ImmutableBytesWritable(row.getBytes()), kv);
		
	}
	
	public static void configureJob(Job job) throws IOException {
		job.setJarByClass(MRToHBaseMapper.class);
		job.setMapperClass(MRToHBaseMapper.class);
		
		TableMapReduceUtil.initTableReducerJob("table", null, job);
		job.setNumReduceTasks(0);
	}
	
	public static void configureJobWithHfileOutputFormat(Job job) throws IOException {
		job.setJarByClass(MRToHBaseMapper.class);
		job.setMapperClass(MRToHBaseMapper.class);
		
		HFileOutputFormat.setOutputPath(job, new Path("/tmp/hfiles-from-hfileoutputformat"));
		job.setNumReduceTasks(0);
	}
	
//	public static void configureWithIncrementalLoad(Job job, HTable htable) throws IOException {
//		job.setJarByClass(MRToHBaseMapper.class);
//		job.setMapperClass(MRToHBaseMapper.class);
//		
//		HFileOutputFormat.setOutputPath(job, new Path("/tmp/hfiles-from-hfileoutputformat-for-incr"));
//		HFileOutputFormat.configureIncrementalLoad(job, htable);
//		job.setNumReduceTasks(0);
//	}
	
}