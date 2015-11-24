import com.sun.corba.se.impl.orbutil.concurrent.Mutex;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Created by prayag on 11/18/15.
 * Distributed Queue consumer. Meant to be run in a thread.
 */
public class ZkQConsumer implements Runnable, QConsumer, Watcher {
    static int id = 0;
    Mutex mutex = new Mutex();
    private int my_id = id;
    private ZooKeeper zk;
    private String qRoot;
    private int messageId = 1;
    private String messagePrefix = "/qMessage";
    private long startTime = -1;
    private long endTime = -1;
    private int consumedCount = 0;
    private final long NANO = (long) 10e8;

    /**
     * Constructor for the consumer
     * @param zk ZooKeeper instance to use
     * @param qRoot The root dir of the queue on ZooKeeper. All messages will be stored under the root.
     */
    public ZkQConsumer(ZooKeeper zk, String qRoot) {
        id++;
        my_id = id;
        this.zk = zk;
        this.qRoot = qRoot;
    }

    /**
     * Start the consuming process. Walks through all messages and consumes them.
     * Waits for producer to produce more messages if runs out of messages to consume.
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void startConsuming() throws KeeperException, InterruptedException {
        if(startTime == -1) startTime = System.nanoTime();
        mutex.acquire();
        while (true) {
            try {
                if(zk.exists(qRoot + messagePrefix + messageId, this) == null){
                    break; //producer hasn't produced this message yet
                }
                zk.getData(qRoot + messagePrefix + messageId, false, null); //throwing away the consumed data
                consumedCount++;
                messageId++;
            } catch (KeeperException.NoNodeException e) {
                System.out.println("ERROR: Expected message not found!");
            }
            //System.out.println(my_id + ": Consumed a total of " + consumedCount + " messages");
        }
        endTime = System.nanoTime();
        mutex.release();
    }

    public long getThrougput(){
        if(endTime == startTime) return Integer.MAX_VALUE;
        return consumedCount * NANO/(endTime - startTime);
    }

    /**
     * callback for ZooKeeper events
     * @param watchedEvent event that we registered for
     */
    public void process(WatchedEvent watchedEvent) {
        //System.out.println("Consumer watcher event!");
        try {
            startConsuming();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the consuming started. Implemented to support threading.
     */
    public void run() {
        try {
            startConsuming();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
