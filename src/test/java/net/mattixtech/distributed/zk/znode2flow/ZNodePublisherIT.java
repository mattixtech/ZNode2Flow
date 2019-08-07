package net.mattixtech.distributed.zk.znode2flow;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:matt@mattixtech.net">Matt Brooks</a>
 */
public class ZNodePublisherIT {
    private final TestingServer testingServer = new TestingServer();
    private String testConnectionString;

    public ZNodePublisherIT() throws Exception {
    }

    @Before
    public void setup() {
        testConnectionString = testingServer.getConnectString();
    }

    @Test
    public void canReceiveSubmission() {
        ZNodePublisher zNodePublisher = ZNodePublisher.withCachedCurator(testConnectionString, "/canReceiveSubmission");

        Subscriber s = new Subscriber();
        zNodePublisher.subscribe(s);

        String val = "Hello World!";
        zNodePublisher.submit(val.getBytes());

        await().atMost(5, TimeUnit.SECONDS).until(() -> s.getReceived().get(0).equals(val));
        zNodePublisher.close();
    }

    @Test
    public void canReceiveAllSubmissions() {
        ZNodePublisher zNodePublisher = ZNodePublisher.withCachedCurator(testConnectionString, "/canReceiveAllSubmissions");

        Subscriber s1 = new Subscriber();
        Subscriber s2 = new Subscriber();
        zNodePublisher.subscribe(s1);
        zNodePublisher.subscribe(s2);

        List<String> toSubmit = List.of("a", "b", "c", "d");
        toSubmit.forEach(str -> {
            zNodePublisher.submit(str.getBytes());
            // Wait so that each subscriber can see every change
            await().atMost(1, TimeUnit.SECONDS).until(() -> s1.getReceived().contains(str) &&
                    s2.getReceived().contains(str));
        });

        assertThat(s1.getReceived(), equalTo(toSubmit));
        assertThat(s2.getReceived(), equalTo(toSubmit));
        zNodePublisher.close();
    }

    private static class Subscriber implements Flow.Subscriber<byte[]> {
        private List<String> received = new ArrayList<>();
        private Flow.Subscription subscription;
        private boolean completed;
        private Throwable error;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            this.subscription.request(1);
        }

        @Override
        public void onNext(byte[] item) {
            received.add(new String(item));
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
        }

        @Override
        public void onComplete() {
            completed = true;
        }

        public List<String> getReceived() {
            return received;
        }

        public boolean isCompleted() {
            return completed;
        }

        public Throwable getError() {
            return error;
        }
    }
}
