import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;
import org.junit.Test;


public class MRToHBaseMapperTest {
	
	Logger logger = Logger.getLogger(MRToHBaseMapperTest.class);
	
	@Test
	public void testJob() throws Exception {
	
		HBaseTestingUtility testingUtility = new HBaseTestingUtility();
		testingUtility.startMiniCluster();
		testingUtility.getDFSCluster().getFileSystem().copyFromLocalFile(new Path("src/test/resources/ga-hourly.txt"), new Path("/tmp/file"));
		testingUtility.createTable("table".getBytes(), "data".getBytes());
		
		Job job = new Job(testingUtility.getConfiguration(), "file to hbase");
//		job.setWorkingDirectory(new Path("src/test/resources/"));
		FileInputFormat.addInputPath(job, new Path("/tmp/file"));
		
		MRToHBaseMapper.configureJob(job);
		
		job.waitForCompletion(true);
		
		HTable hTable = new HTable(testingUtility.getConfiguration(), "table".getBytes());
		ResultScanner scanner = hTable.getScanner("data".getBytes());
		Result next = null;
		logger.debug("scanning the the table");
		while((next = scanner.next()) != null) {
			logger.debug(String.format("%s %s", next.getRow(), next.getValue("data".getBytes(), "d".getBytes())));
			System.out.println(String.format("%s %s", new String(next.getRow()), next.getValue("data".getBytes(), "d".getBytes())));
		}
	}
	
}
