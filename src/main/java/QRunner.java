import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by prayag on 11/19/15.
 * <p></p>
 * Runner class for the distributed queue simulation.
 */
public class QRunner implements RunnableTest, Watcher {
    private String hostAndPort;
    private String qRoot = "/qRootB";
    private int timeOut = 1000;
    private int numThreads = 1;
    private int timeout = 1000;

    /**
     * Constructor for QRunner
     * @param hostAndPort host and port number for the ZooKeeper client
     * @param numThreads number of threads to be used for simulation
     */
    public QRunner(String hostAndPort, int numThreads) {
        this.hostAndPort = hostAndPort;
        this.numThreads = numThreads;
    }

    /**
     * Creates consumer and producer threads and runs all of them in parallel and awaits the results
     * Creates separate ZooKeeper instances to feed each of the consumers and receivers
     *
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     */

    public void runTest() throws KeeperException, InterruptedException, IOException {
        ZooKeeper zk = new ZooKeeper(hostAndPort, timeOut, this);
        List<Long> throughputs = new ArrayList<Long>();
        try {
            zk.create(qRoot, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
        }

        List<ZooKeeper> pZks = new ArrayList<ZooKeeper>();
        List<ZooKeeper> cZks = new ArrayList<ZooKeeper>();
        List<ZkQProducer> ps = new ArrayList<ZkQProducer>(); //Producers
        List<ZkQConsumer> cs = new ArrayList<ZkQConsumer>(); //Producers
        List<Thread> pThreads = new ArrayList<Thread>(); //Producer threads
        List<Thread> cThreads = new ArrayList<Thread>(); //Consumer threads

        for (int i = 0; i <= numThreads / 3; i++) { //keep the number of producers to a third of number of consumers
            pZks.add(new ZooKeeper(hostAndPort, timeout, this));
        }

        for (int i = 0; i < numThreads; i++) {
            cZks.add(new ZooKeeper(hostAndPort, timeout, this));
        }

        for (int i = 0; i <= numThreads / 3; i++) { //keep the number of producers to a third of number of consumers
            ZkQProducer p = new ZkQProducer(pZks.get(i), qRoot);
            ps.add(p);
            Thread pThread = new Thread(p);
            cThreads.add(pThread);
            pThread.start();
        }

        for (int i = 0; i < numThreads; i++) {
            ZkQConsumer c = new ZkQConsumer(cZks.get(i), qRoot);
            cs.add(c);
            Thread cThread = new Thread(c);
            cThreads.add(cThread);
            cThread.start();
        }

        for (int i = 0; i <= numThreads / 3; i++) {
            for (Thread t : pThreads) {
                t.join();
            }
        }

        for (int i = 0; i < numThreads; i++) {
            for (Thread t : cThreads) {
                t.join();
            }
        }

        for (ZooKeeper tmp : cZks) {
            tmp.close();
        }

        for (ZooKeeper tmp : pZks) {
            tmp.close();
        }
        zk.close();

        long cTSum = 0;
        for (ZkQConsumer c : cs) {
            cTSum += c.getThrougput();
        }

        System.out.println("Average consumer throughput: " + (double) cTSum / cs.size() + " messages per second");
    }

    /**
     * callback for ZooKeeper events
     * @param watchedEvent event that we registered for
     */
    public void process(WatchedEvent watchedEvent) {
        //System.out.println("Watcher Event Happened");
    }
}
