import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.util.CharsetUtil;
import sun.nio.cs.StandardCharsets;

import java.nio.charset.Charset;
import java.nio.charset.spi.CharsetProvider;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by prayag on 11/18/15.
 * <p></p>
 * ReadWrite test class. All tests are tightly coupled as one test.
 */
public class SyncReadWrite implements Runnable{
    private ZooKeeper zk;
    private int reps = 100;
    private long MILLI = (long) 10e8;
    private String znodeName = "/TestNode";
    private String znodeData = "TestData";

    /**
     * Constructor for the test class
     * @param zk ZooKeeper instance to use
     * @param znodeName The root dir of the test.
     */
    public SyncReadWrite(ZooKeeper zk, String znodeName) {
        this.zk = zk;
        this.znodeName = znodeName;
    }

    /**
     * Start the test. Creates data nodes, reads messages off the data nodes,
     * writes new data to the data nodes and deletes the data nodes. Reports
     * performance for each operation.
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void runHelper() throws KeeperException, InterruptedException {
        long startTime= System.nanoTime();
        for (int i = 0; i < reps; i++) {
            zk.create(znodeName + i, znodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        long timeTaken = System.nanoTime() - startTime;
        System.out.println((float) reps * MILLI / timeTaken + " creates per second");

        startTime = System.nanoTime();
        for (int i = 0; i < reps; i++) {
            byte[] data = zk.getData(znodeName + i, false, new Stat());
            String dataString = new String(data, CharsetUtil.US_ASCII);
            if (dataString.equals(znodeData) == false) {
                System.out.println("Bad data! " + dataString + ", " + znodeData);
            }
        }
        timeTaken = System.nanoTime() - startTime;
        System.out.println((float) reps * MILLI / timeTaken + " reads per second");

        startTime = System.nanoTime();
        for (int i = 0; i < reps; i++) {
            zk.setData(znodeName + i, (znodeData + "New").getBytes(), -1);
        }
        timeTaken = System.nanoTime() - startTime;
        System.out.println((float) reps * MILLI / timeTaken + " writes per second");

        startTime = System.nanoTime();
        for (int i = 0; i < reps; i++) {
            zk.delete(znodeName + i, -1);
        }
        timeTaken = System.nanoTime() - startTime;
        System.out.println((float) reps * MILLI / timeTaken + " deletes per second");
    }

    /**
     * Get the test started. Implemented to support threading.
     */
    public void run() {
        try {
            runHelper();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}