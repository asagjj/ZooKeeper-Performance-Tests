import com.sun.corba.se.impl.orbutil.concurrent.Mutex;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.util.Collections;
import java.util.List;

/**
 * Created by prayag on 11/19/15.
 * Distributed Queue producer. Meant to be run in a thread.
 */
public class ZkQProducer implements QProducer, Runnable {
    private ZooKeeper zk;
    private static int id = 0;
    private int myId = 0;
    private String qRoot;
    private Mutex mutex = new Mutex();
    private int numMessages = 10;
    private int messageId = 1;
    private String messageData = "TestData";
    private String messagePrefix = "/qMessage";

    /**
     * Constructor for the producer
     * @param zk ZooKeeper instance to use
     * @param qRoot The root dir of the queue on ZooKeeper. All messages will be stored under the root.
     */
    public ZkQProducer(ZooKeeper zk, String qRoot) {
        id++;
        myId = id;
        this.zk = zk;
        this.qRoot = qRoot;
    }

    /**
     * Start producing messages
     */
    public void startProducing() {
        for (int i = 0; i < numMessages; i++) {
            try {
                zk.create(qRoot + messagePrefix + messageId, messageData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (InterruptedException e) {
            } catch (KeeperException e) {
                //someone else produced this message before us
            }
            messageId++;
        }
        //System.out.println(myId + ": Produced " + numMessages + " messages");
    }

    /**
     * Get the producing started. Implemented to support threading.
     */
    public void run() {
        startProducing();
    }
}
