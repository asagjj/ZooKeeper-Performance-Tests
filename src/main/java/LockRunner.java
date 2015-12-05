import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by prayag on 11/22/15.
 * Runner class for distributed locking simulation
 */
public class LockRunner implements RunnableTest, Watcher {
    private String[] hostsAndPorts;
    private String lRoot = "/locksA";
    private String rootPrefix = "/lockG";
    private int numThreads = 1;
    private int timeout = 100;
    private final long NANO = (long) 10e8;
    private final long MILLI = (long) 10e5;

    /**
     * Constructor for SyncReadWriteRunner
     *
     * @param hostAndPort host and port number for the ZooKeeper client
     * @param numThreads  number of threads to be used for simulation
     */
    public LockRunner(String[] hostAndPort, int numThreads) {
        this.numThreads = numThreads;
        this.hostsAndPorts = hostAndPort;
    }

    private void fillRoots(String[] lRoots) {
        for (int i = 0; i < lRoots.length; i++) {
            lRoots[i] = rootPrefix + Integer.toString(i);
        }
    }

    /**
     * Creates LockClient tests and runs them in parallel
     * Creates separate ZooKeeper instances to feed each of the tests
     */
    public void runTest() throws KeeperException, InterruptedException, IOException {
        Thread[] threads = new Thread[numThreads];
        LockClient[] lockClients = new LockClient[numThreads];
        ZooKeeper[] zks = new ZooKeeper[numThreads];
        for (int i = 0; i < numThreads; i++) {
            zks[i] = new ZooKeeper(hostsAndPorts[i % hostsAndPorts.length], timeout, this);
        }
        try {
            zks[0].create(lRoot, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
        }

        for (int i = 0; i < numThreads; i++) {
            lockClients[i] = new LockClient(zks[i], lRoot);
        }

        long startTime = System.nanoTime();
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(lockClients[i]);
            threads[i] = t;
            t.start();
        }
        //Thread.sleep(1000);
        for (Thread t : threads) t.join();
        System.out.println("Acquired " + numThreads + " locks in " + (System.nanoTime() - startTime) / MILLI + " milli seconds");
        for(ZooKeeper zk : zks) zk.close();
    }

    /**
     * callback for ZooKeeper events
     *
     * @param watchedEvent event that we registered for
     */
    public void process(WatchedEvent watchedEvent) {
        //System.out.println("Runner watcher event.");
    }
}
