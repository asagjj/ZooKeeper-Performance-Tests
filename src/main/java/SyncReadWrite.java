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
    private int reps = 1000;
    private int readReps = reps * 1000;
    private long NANO = (long) 10e8;
    private String znodeName;
    private String znodeData = "TestData";
    private double createTP;
    private double readTP;
    private double writeTP;
    private double deleteTP;

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
        createTP = (double) reps * NANO / timeTaken;
        //System.out.println((double) reps * NANO / timeTaken + " creates per second");

        startTime = System.nanoTime();
        for (int i = 0; i < readReps; i++) {
            byte[] data = zk.getData(znodeName + (i%reps), false, new Stat());
            String dataString = new String(data, CharsetUtil.US_ASCII);
            if (dataString.equals(znodeData) == false) {
                System.out.println("Bad data! " + dataString + ", " + znodeData);
            }
        }
        timeTaken = System.nanoTime() - startTime;
        readTP = (double) readReps * NANO / timeTaken;
        //System.out.println((double) readReps * NANO / timeTaken + " reads per second");

        startTime = System.nanoTime();
        for (int i = 0; i < reps; i++) {
            zk.setData(znodeName + i, (znodeData + "New").getBytes(), -1);
        }
        timeTaken = System.nanoTime() - startTime;
        writeTP = (double) reps * NANO / timeTaken;
        //System.out.println((double) reps * NANO / timeTaken + " writes per second");

        startTime = System.nanoTime();
        for (int i = 0; i < reps; i++) {
            zk.delete(znodeName + i, -1);
        }
        timeTaken = System.nanoTime() - startTime;
        deleteTP = (double) reps * NANO / timeTaken;
        //System.out.println((double) reps * NANO / timeTaken + " deletes per second");
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

    public double getCreateTP(){
        return createTP;
    }

    public double getWriteTP() {
        return writeTP;
    }

    public double getDeleteTP() {
        return deleteTP;
    }

    public double getReadTP() {
        return readTP;
    }
}