import org.apache.zookeeper.*;

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
    private String zRoot = "/TestNodeD";
    private String znodeName = zRoot + "/Test";
    private String[] hostsAndPorts;

    /**
     * Constructor for SyncReadWriteRunner
     * @param hostsAndPorts host and port number for the ZooKeeper client
     * @param numThreads number of threads to be used for simulation
     */
    public SyncReadWriteRunner(String[] hostsAndPorts, int numThreads) throws IOException {
        this.hostsAndPorts = hostsAndPorts;
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
        ZooKeeper[] zks = new ZooKeeper[numThreads];
        SyncReadWrite[] tests = new SyncReadWrite[numThreads];
        List<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < numThreads; i++) {
            zks[i] = new ZooKeeper(hostsAndPorts[i % hostsAndPorts.length], timeout, this);
        }

        try {
            zks[0].create(zRoot, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
        }

        for (int i = 0; i < numThreads; i++) {
            SyncReadWrite test = new SyncReadWrite(zks[i], znodeName + i);
            tests[i] = test;
            Thread t = new Thread(test);
            threads.add(t);
            t.start();
        }

        for (int i = 0; i < numThreads; i++) {
            for (Thread t : threads) {
                t.join();
            }
        }

        double rwTSum = 0;
        for (SyncReadWrite test : tests) {
            rwTSum += test.getCreateTP();
        }
        System.out.println("Average create throughput: " + (double) rwTSum / numThreads + " creates per second");

        rwTSum = 0;
        for (SyncReadWrite test : tests) {
            rwTSum += test.getReadTP();
        }
        System.out.println("Average read throughput: " + (double) rwTSum / numThreads + " reads per second");

        rwTSum = 0;
        for (SyncReadWrite test : tests) {
            rwTSum += test.getWriteTP();
        }
        System.out.println("Average write throughput: " + (double) rwTSum / numThreads + " writes per second");

        rwTSum = 0;
        for (SyncReadWrite test : tests) {
            rwTSum += test.getDeleteTP();
        }
        System.out.println("Average delete throughput: " + (double) rwTSum / numThreads + " deletes per second");

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
        //System.out.println("Watcher Event Happened");
    }
}
