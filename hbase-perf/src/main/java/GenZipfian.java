import generator.IntegerGenerator;
import generator.ScrambledZipfianGenerator;


public class GenZipfian {

//  static final Log LOG = LogFactory.getLog(IncrementBlaster.class);
  static int cols = 100000;

  public static void main(String argv[]) {
    IntegerGenerator gen = new ScrambledZipfianGenerator(cols); // zipfian where heavy entries are distributed

    int count = 100000;
    for (int i = 0; i < count; i++) {
        int val = gen.nextInt();
        System.out.println(String.format("%08d%052d", val, val));
    }
    return;
  }
  
}
