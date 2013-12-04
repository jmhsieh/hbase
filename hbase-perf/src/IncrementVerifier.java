import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IncrementVerifier implements Tool {
  static final Log LOG = LogFactory.getLog(IncrementVerifier.class);
  Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration arg0) {
    this.conf = arg0;

  }

  @Override
  public int run(String[] arg0) throws Exception {
    HTable t = new HTable("test");
    Scan s = new Scan();
    ResultScanner rs = t.getScanner(s);

    Result r;
    while ((r = rs.next()) != null) {
      long rowsum = 0;
      String row = Bytes.toString(r.getRow());
      NavigableMap<byte[], byte[]> cols = r.getFamilyMap(Bytes.toBytes("f"));
      for (Map.Entry<byte[], byte[]> e : cols.entrySet()) {
        long val = Bytes.toLong(e.getValue());
        LOG.info(row + " fh:" + Bytes.toString(e.getKey()) + " " +  val);
        rowsum += val;
      }
      LOG.info("rowsum = " + rowsum);
    }

    return 0;
  }

  public static void main(String argv[]) throws Exception {
    try {
      int ret = ToolRunner.run(HBaseConfiguration.create(), new IncrementVerifier(), argv);
      System.exit(ret);
    } catch (Exception e) {
      LOG.error("Make sure you have a table named 'test' with a cf 'f' present.", e);
      System.exit(1);
    }
  }

}
