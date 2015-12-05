import com.sun.corba.se.impl.orbutil.concurrent.Mutex;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by prayag on 11/22/15.
 * <p></p>
 * LockClient class to simulate distributed locking
 */
public class LockClient implements Runnable, Watcher {
    private ZooKeeper zk;
    private static int id = 0;
    private int myId = 0;
    private String qRoot;
    private Mutex mutex = new Mutex();
    private int numMessages = 10;
    private int messageId = 1;
    private String messageData = "TestData";
    private String lockPrefix = "/lock";
    private String lRoot;
    private List<String> lockedPaths;
    CountDownLatch latch = new CountDownLatch(1);
    private String createdPath;

    /**
     * Constructor for the lock client
     *
     * @param zk     ZooKeeper instance to use
     * @param lRoot The root dirs of the locking protocol on ZooKeeper.
     */
    public LockClient(ZooKeeper zk, String lRoot) {
        id++;
        myId = id;
        this.zk = zk;
        this.lRoot = lRoot;
        //lockedPaths = new ArrayList<String>(lRoots.length);
    }

    /**
     * Obtain all the locks and release them.
     */
//    private void getAllLocks() throws KeeperException, InterruptedException {
//        for (String lRoot : lRoots) {
//            if (getLock(lRoot) == false) {
//               return;
//            }
//            System.out.println(myId + ": calling getAllLocks");
//        }
//        Thread.sleep(100);
//        removeAllLocks();
//        System.out.println(myId + ": Got all locks");
//    }

    private void unlock() throws KeeperException, InterruptedException {
        zk.delete(createdPath, -1);
        //System.out.println(myId + ": Removing " + createdPath);
    }

    /**
     * Obtain all the locks and release them.
     *
     * @param lRoot root node for the lock needed
     */
    private boolean getLock(String lRoot) throws KeeperException, InterruptedException {
        String[] splitPath = createdPath.split("/");
        String createdChild = splitPath[splitPath.length - 1];
        List<String> children = zk.getChildren(lRoot, false);
        Collections.sort(children);
        int idx = Collections.binarySearch(children, createdChild);
        if (idx == 0) {
            //System.out.println(myId + ": Got the lock for " + createdPath);
            return true;
        } else {
            //System.out.println(myId + ": Couldn't get the lock for " + createdPath);
            //System.out.println(myId + ": Watching " + lRoot + "/" + children.get(idx-1) + ", " + createdPath);
            Stat stat = zk.exists(lRoot + "/" + children.get(idx - 1), this);
            if(stat == null){
                latch.countDown();
            }
            //System.out.println(myId + ": Watching " + stat.toString());
            //latch.await();
            //zk.delete(lRoot + "/" + children.get(idx-1), -1);
        }
        return false;
    }

    /**
     * Get the locking started. Implemented to support threading.
     */
    public void run() {
        try {
            String path = lRoot + lockPrefix;
            createdPath = zk.create(path, messageData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            if(getLock(lRoot) == true){
                //Thread.sleep(10); //Doing CS
                unlock();
            }
            else{
                //System.out.println(myId + ": countdown event");
                latch.await();
                if(getLock(lRoot) == true) {
                    unlock();
                }
            }
        } catch (KeeperException e) {
            System.out.println(myId + ": Exception thrown");
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println(myId + ": Exception thrown");
            e.printStackTrace();
        }
    }


    /**
     * callback for ZooKeeper events
     *
     * @param watchedEvent event that we registered for
     */
    public void process(WatchedEvent watchedEvent) {
        //System.out.println(myId + ": watcher event in LockClient");
        latch.countDown();
        //try {
        //    if(getLock(lRoot)) {
        //        //Thread.sleep(10); //Doing CS
        //        unlock();
        //    }
        //} catch (KeeperException e) {
        //    System.out.println(myId + ": Exception thrown");
        //    e.printStackTrace();
        //} catch (InterruptedException e) {
        //    System.out.println(myId + ": Exception thrown");
        //    e.printStackTrace();
        //}
    }
}
