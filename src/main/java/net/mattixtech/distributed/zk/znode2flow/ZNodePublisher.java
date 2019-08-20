package net.mattixtech.distributed.zk.znode2flow;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * @author Matt Brooks
 */
public class ZNodePublisher implements Flow.Publisher<byte[]> {
    private final SubmissionPublisher<byte[]> publisher = new SubmissionPublisher<>(new ForkJoinPool(Math.max(1,
            Runtime.getRuntime().availableProcessors() - 1)), Flow.defaultBufferSize());
    private final CuratorFramework curator;
    private final NodeCache nodeCache;
    private final String zkPath;
    private static final Map<String, CuratorFramework> cachedCurators = new HashMap<>();

    private ZNodePublisher(CuratorFramework curator, String zkPath) {
        this.curator = Objects.requireNonNull(curator);
        this.zkPath = PathUtils.validatePath(zkPath);

        if (this.curator.getState() != CuratorFrameworkState.STARTED)
            this.curator.start();
        CuratorZookeeperClient zkClient = curator.getZookeeperClient();

        try {
            zkClient.blockUntilConnectedOrTimedOut();
            nodeCache = new NodeCache(curator, this.zkPath);
            nodeCache.getListenable().addListener(this::onNodeChange);
            nodeCache.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ZNodePublisher withCachedCurator(String connectionString, String zkPath) {
        return new ZNodePublisher(startCachedCurator(connectionString), zkPath);
    }

    public static ZNodePublisher withProvidedCurator(CuratorFramework curator, String zkPath) {
        return new ZNodePublisher(curator, zkPath);
    }

    private synchronized static CuratorFramework startCachedCurator(String zkConnectionString) {
        if (!cachedCurators.containsKey(zkConnectionString)) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(zkConnectionString),
                    "connection string cannot be null or empty");
            CuratorFramework curator = CuratorFrameworkFactory.newClient(zkConnectionString,
                    new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
            curator.start();
            cachedCurators.put(zkConnectionString, curator);
        }

        return cachedCurators.get(zkConnectionString);
    }

    private void onNodeChange() {
        publisher.submit(nodeCache.getCurrentData().getData());
    }

    public synchronized void submit(byte[] newValue) {
        try {
            // Upsert by making sure the path is created already
            //
            // Note the upsert here is not fully atomic. If the ZNode is deleted between mkdirs() and
            // setData() an exception will be thrown. It is up to the user to guard their calls to submit()
            // and any deletes of this node with a ZooKeeper lock.
            ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), zkPath);
            curator.setData().forPath(zkPath, newValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void close() {
        publisher.close();
        CuratorFramework cf = cachedCurators.get(zkPath);

        if (cf != null) {
            cf.close();
        }
    }

    @Override
    public synchronized void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
        publisher.subscribe(subscriber);
    }
}
