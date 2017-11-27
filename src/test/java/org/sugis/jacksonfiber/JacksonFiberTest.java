package org.sugis.jacksonfiber;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.FiberExecutorScheduler;
import co.paralleluniverse.strands.Strand;

public class JacksonFiberTest {
    private static final Logger LOG = LoggerFactory.getLogger(JacksonFiberTest.class);
    private static final int SCATTER_SIZE = 32;
    private static final int MAX_DEPTH = 10000;

    private final Random r = new Random();

    @Test
    public void deserTest() throws Exception {
        final Executor executor = Executors.newSingleThreadExecutor();
        final FiberExecutorScheduler scheduler = new FiberExecutorScheduler("executor", executor);

        final int nFibers = 1000;
        final ObjectMapper mapper = new ObjectMapper();

        final ObjectNode[] testVector = new ObjectNode[nFibers];
        final byte[][] payloads = new byte[nFibers][];

        LOG.info("test vector create");
        for (int p = 0; p < nFibers; p++) {
            ObjectNode ptr = testVector[p] = mapper.createObjectNode();
            for (int i = 0; i < r.nextInt(MAX_DEPTH); i++) {
                final ObjectNode next = mapper.createObjectNode();
                ptr.set("next", next);
                ptr = next;
            }
            payloads[p] = mapper.writeValueAsBytes(testVector[p]);
        }

        final AsyncFiberStream[] stream = new AsyncFiberStream[nFibers];
        final List<Iterator<ByteBuffer>> bufIters = new ArrayList<>();
        final List<Fiber<JsonNode>> fibers = new ArrayList<>();
        final AtomicInteger fRemaining = new AtomicInteger(nFibers);

        final long fiberStart = System.currentTimeMillis();
        LOG.info("fibers start");
        for (int f = 0; f < nFibers; f++) {
            final InputStream s = stream[f] = new AsyncFiberStream(f);

            final int f0 = f;
            fibers.add(new Fiber<JsonNode>(scheduler, () -> {
                try {
                    final JsonNode result = mapper.readTree(s);
                    LOG.info("fiber #{} done in {} ({} remain)", f0, since(fiberStart), fRemaining.decrementAndGet());
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).start());
            bufIters.add(new BufIterator(payloads[f]));
        }

        new Fiber<Void>(scheduler, () -> {
            LOG.info("buffers scatter");
            boolean remaining = true;
            while (remaining) {
                remaining = false;
                for (int f = 0; f < nFibers; f++) {
                    final Iterator<ByteBuffer> iter = bufIters.get(f);
                    if (iter.hasNext()) {
                        remaining = true;
                        stream[f].offer(iter.next(), () -> {});
                        Strand.yield();
                    }
                }
            }
        }).start().join();

        for (int f = 0; f < nFibers; f++) {
            LOG.debug("#{} {}", f, fibers.get(f).get(30, TimeUnit.SECONDS));
        }
        LOG.info("test succeeds in {}", since(fiberStart));
    }

    private String since(long when) {
        return String.format("%.2fs", (System.currentTimeMillis() - when) / 1000.0);
    }

    class BufIterator implements Iterator<ByteBuffer> {
        private final byte[] payload;
        private int off = 0;
        BufIterator(byte[] payload) {
            this.payload = payload;
        }
        @Override
        public boolean hasNext() {
            return off < payload.length;
        }

        @Override
        public ByteBuffer next() {
            final int len = Math.min(payload.length - off, r.nextInt(SCATTER_SIZE-1)+1);
            final ByteBuffer next = ByteBuffer.allocate(len);
            next.put(payload, off, len);
            next.position(0);
            off += len;
            return next;
        }
    }
}
