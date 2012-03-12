import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;
import org.junit.Test;


public class HFileWithMRWithIncrementalLoad {
	
	Logger logger = Logger.getLogger(HFileWithMRWithIncrementalLoad.class);
	
	@Test
	public void testJob() throws Exception {
	
		HBaseTestingUtility testingUtility = new HBaseTestingUtility();
		testingUtility.startMiniCluster();
		testingUtility.getDFSCluster().getFileSystem().copyFromLocalFile(new Path("src/test/resources/ga-hourly.txt"), new Path("/tmp/file"));
		testingUtility.createTable("table".getBytes(), "data".getBytes());
		
		Job job = new Job(testingUtility.getConfiguration(), "file to hbase");
//		job.setWorkingDirectory(new Path("src/test/resources/"));
		FileInputFormat.addInputPath(job, new Path("/tmp/file"));
		
		Configuration configuration = testingUtility.getConfiguration();
		HTable hTable = new HTable(configuration, "table");
		MRToHBaseMapper.configureJobWithHfileOutputFormat(job);
		job.setReducerClass(KeyValueSortReducer.class);
		job.setNumReduceTasks(1);
		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		
		job.waitForCompletion(true);
		
//		job = new Job(configuration);
//		FileInputFormat.addInputPath(job, new Path("/tmp/hfiles-from-hfileoutputformat"));
//		HFileOutputFormat.configureIncrementalLoad(job, hTable);
//		
//		job.waitForCompletion(true);
		
		ResultScanner scanner = hTable.getScanner("data".getBytes());
		Result next = null;
		logger.debug("scanning the the table");
		while((next = scanner.next()) != null) {
			logger.debug(String.format("%s %s", next.getRow(), next.getValue("data".getBytes(), "d".getBytes())));
			System.out.println(String.format("%s %s", new String(next.getRow()), next.getValue("data".getBytes(), "d".getBytes())));
		}
	}
	
}
