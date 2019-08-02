package net.mattixtech.distributed.zk.znode2flow;

import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * @author <a href="mailto:matt@mattixtech.net">Matt Brooks</a>
 */
public class FlowingZNode implements Flow.Publisher<byte[]> {
    // TODO use a custom pool
    private final SubmissionPublisher<byte[]> publisher = new SubmissionPublisher<>();
    private final CuratorFramework curator;
    private final NodeCache nodeCache;
    private final String zkPath;
    private final InterProcessLock zkLock;

    public FlowingZNode(CuratorFramework curator, String zkWatchPath, String zkLockPath) {
        this.zkPath = PathUtils.validatePath(zkWatchPath);
        PathUtils.validatePath(zkLockPath);
        
        this.curator = curator;
        CuratorZookeeperClient zkClient = curator.getZookeeperClient();
        zkLock = new InterProcessMutex(curator, zkLockPath);

        try {
            zkClient.blockUntilConnectedOrTimedOut();
            nodeCache = new NodeCache(curator, this.zkPath);
            nodeCache.getListenable().addListener(this::onNodeChange);
            nodeCache.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static CuratorFramework startedCurator(String zkConnectionString) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zkConnectionString),
                "connection string cannot be null or empty");
        CuratorFramework curator = CuratorFrameworkFactory.newClient(zkConnectionString,
                new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
        curator.start();

        return curator;
    }

    private void onNodeChange() {
        publisher.submit(nodeCache.getCurrentData().getData());
    }

    public synchronized void submit(byte[] newValue) {
        try {
            try {
                zkLock.acquire();

                // Do an upsert on the path
                if (curator.checkExists().forPath(zkPath) == null) {
                    curator.create().forPath(zkPath, newValue);
                } else {
                    curator.setData().forPath(zkPath, newValue);
                }
            } finally {
                zkLock.release();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void close() {
        publisher.close();
    }

    @Override
    public synchronized void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
        publisher.subscribe(subscriber);
    }
}
