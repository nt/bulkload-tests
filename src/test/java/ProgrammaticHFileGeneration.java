import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;


public class ProgrammaticHFileGeneration {
	
	static Comparator<byte[]> byteArrComparator = new Comparator<byte[]>() {
		public int compare(byte[] o1, byte[] o2) {
			return Bytes.compareTo(o1, 0, o1.length, o2, 0, o2.length);
		}
	};
	
	@Test
	public void generate() throws Exception {
		InputStream stream = ProgrammaticHFileGeneration.class.getResourceAsStream("ga-hourly.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		String line = null;
		
		Map<byte[], String> rowValues = new HashMap<byte[], String>();
		
		while((line = reader.readLine())!=null) {
			String[] vals = line.split(",");
			String row = new StringBuilder(vals[0]).append(".").append(vals[1]).append(".").append(vals[2]).append(".").append(vals[3]).toString();
			rowValues.put(row.getBytes(), line);
		}
		
		List<byte[]> keys = new ArrayList<byte[]>(rowValues.keySet());
		Collections.sort(keys, byteArrComparator);
		
		
		HBaseTestingUtility testingUtility = new HBaseTestingUtility();
		testingUtility.startMiniCluster();
		
		testingUtility.createTable("table".getBytes(), "data".getBytes());
		
		Writer writer = new HFile.Writer(testingUtility.getTestFileSystem(),
			new Path("/tmp/hfiles/data/hfile"),
			HFile.DEFAULT_BLOCKSIZE, Compression.Algorithm.NONE, KeyValue.KEY_COMPARATOR);
		
		for(byte[] key:keys) {
			writer.append(new KeyValue(key, "data".getBytes(), "d".getBytes(), rowValues.get(key).getBytes()));
		}
		
		writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
		writer.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes.toBytes(true));
		writer.close();
		
		Configuration conf = testingUtility.getConfiguration();
		
		LoadIncrementalHFiles loadTool = new LoadIncrementalHFiles(conf);
		HTable hTable = new HTable(conf, "table".getBytes());
		
		loadTool.doBulkLoad(new Path("/tmp/hfiles"), hTable);
		
		ResultScanner scanner = hTable.getScanner("data".getBytes());
		Result next = null;
		System.out.println("Scanning");
		while((next = scanner.next()) != null) {
			System.out.format("%s %s\n", new String(next.getRow()), new String(next.getValue("data".getBytes(), "d".getBytes())));
		}
		
		
	}
	
}
