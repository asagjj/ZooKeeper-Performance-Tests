import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by prayag on 11/18/15.
 * The Runner class for distributed file system simulation tests.
 */
public class SyncReadWriteRunner implements RunnableTest, Watcher {
    private int timeout = 1000;
    private int numThreads;
    private String znodeName = "/TestNode/Test";
    private String hostAndPort;

    /**
     * Constructor for SyncReadWriteRunner
     * @param hostAndPort host and port number for the ZooKeeper client
     * @param numThreads number of threads to be used for simulation
     */
    public SyncReadWriteRunner(String hostAndPort, int numThreads) throws IOException {
        this.hostAndPort = hostAndPort;
        this.numThreads = numThreads;
    }

    /**
     * Creates ReadWrite tests and runs them in parallel
     * Creates separate ZooKeeper instances to feed each of the tests
     *
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     */
    public void runTest() throws KeeperException, InterruptedException, IOException {
        List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
        List<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < numThreads; i++) {
            zks.add(new ZooKeeper(hostAndPort, timeout, this));
        }

        for (int i = 0; i < numThreads; i++) {
            SyncReadWrite test = new SyncReadWrite(zks.get(i), znodeName + i);
            Thread t = new Thread(test);
            threads.add(t);
            t.start();
        }

        for (int i = 0; i < numThreads; i++) {
            for (Thread t : threads) {
                t.join();
            }
        }

        for (ZooKeeper zk : zks) {
            zk.close();
        }
    }

    /**
     * callback for ZooKeeper events
     *
     * @param watchedEvent event that we registered for
     */
    public void process(WatchedEvent watchedEvent) {
        System.out.println("Watcher Event Happened");
    }
}
