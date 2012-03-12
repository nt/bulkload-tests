import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;
import org.junit.Test;


public class HFileWithMRTest {
	
	Logger logger = Logger.getLogger(HFileWithMRTest.class);
	
	@Test
	public void testJob() throws Exception {
	
		HBaseTestingUtility testingUtility = new HBaseTestingUtility();
		testingUtility.startMiniCluster();
		testingUtility.getTestFileSystem().copyFromLocalFile(new Path("src/test/resources/ga-hourly.txt"), new Path("/tmp/file"));
		
		Job job = new Job(testingUtility.getConfiguration(), "file to hbase");
//		job.setWorkingDirectory(new Path("src/test/resources/"));
		FileInputFormat.addInputPath(job, new Path("/tmp/file"));
		
		MRToHBaseMapper.configureJobWithHfileOutputFormat(job);
		
		job.waitForCompletion(true);
		
		testingUtility.getTestFileSystem().copyToLocalFile(new Path("/tmp/hfiles-from-hfileoutputformat"), new Path("/tmp"));
	}

}
