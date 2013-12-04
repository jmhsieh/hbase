import generator.CounterGenerator;
import generator.IntegerGenerator;
import generator.ScrambledZipfianGenerator;
import generator.UniformIntegerGenerator;
import generator.ZipfianGenerator;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class allows the user to do a performance of the increment operation on
 * combinations of cols, rows, and works with 1, 10, 20, 40, and 80 threads
 * 
 * Arguments are passed via -Dxxxx=yyyy configuration settings
 * 
 * rows=number of rows to write to (default 1) cols=number of cols to increment
 * per row (default 1) incrs=number of total increment operations to perform
 * (default 250000) dist= SZIPF|ZIPF|UNIFORM|SEQ (type of distribution, SZipf is
 * scrambled Zipf) colPerIncr=number of cols per incr rpc call made. (default 1)
 * 
 * TODO: This currently assumes there is a table 'test' with colunm family 'f'
 * defined.
 */
public class IncrementBlaster implements Tool {
  static final Log LOG = LogFactory.getLog(IncrementBlaster.class);
  Configuration conf = null;
  int incrCount = 250000;
  boolean writeToWAL = false;

  enum GenType {
    SZIPF, ZIPF, UNIFORM, SEQ
  };

  GenType dist;
  int colPerIncr = 1;
  int threads = 0; // if 0 do 1,10,20,40,80 threads, if specified just do the
                   // one run

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  IntegerGenerator getGenerator(int cols) {
    switch (dist) {
    case SEQ:
      return new CounterGenerator(0); // counter starting from 0
    case SZIPF:
      return new ScrambledZipfianGenerator(cols); // zipfian where heavy entries
                                                  // are distributed
    case ZIPF:
      return new ZipfianGenerator(cols); // zipfian where earlier entries are
                                         // heavier
    case UNIFORM:
      return new UniformIntegerGenerator(0, cols); // uniform random generator.
    default:
      return null;
    }
  }

  long val(Increment inc, byte[] col) {
    NavigableMap<byte[], Long> colmap = inc.getFamilyMapOfLongs().get("f".getBytes());
    if (colmap == null)
      return 0;

    Long v = colmap.get(col);
    if (v == null)
      return 0;
    return v.longValue();
  }

  int doBatch(HTableInterface t, int incrCount, int rows, int cols) throws IOException {
    IntegerGenerator gen = getGenerator(cols);
    for (int i = 0; i < incrCount; i++) {
      // increment the same row, colfam, col
      byte[] row = Bytes.toBytes(String.format("%08d", i % rows));
      Increment inc = new Increment(row);
      for (int j = 0; j < colPerIncr; j++) {
        // if colPerIncr > 1, we are putting multiple increments into a
        // increment call.
        int randcol = gen.nextInt() % cols;
        byte[] col = String.format("%05d", randcol % cols).getBytes();
        long v = val(inc, col);
        inc.addColumn("f".getBytes(), col, v + 1);
        // if we add more than one increment we need to count it against the
        // increment total.
        if (j != 0) {
          i++;
        }
      }
      inc.setDurability(writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL);

      t.increment(inc);
    }
    return incrCount;
  }

  int runMultiThread(final int threads, final int rows, final int cols) throws IOException,
      InterruptedException {
    LOG.info("Opening table @ 0 ");
    final HTablePool pool = new HTablePool();
    final CountDownLatch waiting = new CountDownLatch(threads), done = new CountDownLatch(threads);
    final int incPerThread = incrCount / threads;

    long start = System.currentTimeMillis();
    LOG.info("Incrementing @ " + (System.currentTimeMillis() - start) + " || " + threads
        + " threads");
    for (int i = 0; i < threads; i++) {
      new Thread() {
        public void run() {
          try {
            waiting.await();
            doBatch(pool.getTable("test"), incPerThread, rows, cols);
          } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } finally {
            done.countDown();
          }
        }
      }.start();
      waiting.countDown();
    }
    done.await();
    long delta = System.currentTimeMillis() - start;
    LOG.info("Done incrs   @ " + delta + "  x " + incrCount + " increments ( "
        + ((float) incrCount / delta * 1000) + " inc/s)" + " || " + threads + " threads, " + cols
        + " cols, " + rows + " rows, " + colPerIncr + " cols/incr rpc");
    pool.close();
    delta = System.currentTimeMillis() - start;
    LOG.info("Closed       @ " + delta);
    return 0;
  }

  @Override
  public int run(String[] arg0) throws Exception {

    writeToWAL = getConf().getBoolean("writeToWAL", true);
    int cols = getConf().getInt("cols", 1);
    int rows = getConf().getInt("rows", 1);
    incrCount = getConf().getInt("incrs", 250000);
    dist = GenType.valueOf(getConf().get("dist", GenType.SZIPF.name()).trim());
    colPerIncr = getConf().getInt("colPerIncr", 1);
    threads = getConf().getInt("threads", 0);

    LOG.info("writeToWAL = " + writeToWAL);
    LOG.info("cols       = " + cols);
    LOG.info("rows       = " + rows);
    LOG.info("colPerIncr = " + colPerIncr);
    LOG.info("incrs total= " + incrCount);
    LOG.info("Distribution = " + dist);

    // threads, cols, rows
    if (threads == 0) {
      runMultiThread(1, rows, cols);
      runMultiThread(10, rows, cols);
      runMultiThread(20, rows, cols);
      runMultiThread(40, rows, cols);
      runMultiThread(80, rows, cols);
    } else {
      runMultiThread(threads, rows, cols);
    }

    return 0;
  }

  public static void main(String argv[]) throws Exception {
    try {
      int ret = ToolRunner.run(HBaseConfiguration.create(), new IncrementBlaster(), argv);
      System.exit(ret);
    } catch (Exception e) {
      LOG.error("Make sure you have a table named 'test' with a cf 'f' present.", e);
      System.exit(1);
    }
  }

}
