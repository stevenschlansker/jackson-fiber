package org.sugis.jacksonfiber;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.function.ToIntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;

class AsyncFiberStream extends InputStream {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncFiberStream.class);
    private final Channel<Chunk> channel = Channels.newChannel(0, OverflowPolicy.BLOCK, false, true);
    private final int idx;
    private Chunk readTop;

    AsyncFiberStream(int idx) {
        this.idx = idx;
    }

    void offer(ByteBuffer content, Runnable callback) throws SuspendExecution {
        if (channel.isClosed()) {
            LOG.info("discarding offer {}", content.remaining());
            callback.run();
            return;
        }
        LOG.debug("send #{} {}", idx, content.remaining());
        try {
            channel.send(new Chunk(content, callback));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e.getMessage());
        }
    }

    @Suspendable
    @Override
    public int read() throws IOException {
        return read0(chunk -> chunk.buf.get());
    }

    @Suspendable
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return read0(chunk -> {
            final int r = Math.min(chunk.buf.remaining(), len);
            chunk.buf.get(b, off, r);
            return r;
        });
    }

    @Suspendable
    private int read0(ToIntFunction<Chunk> reader) throws IOException {
        try {
            if (readTop == null) {
                LOG.debug("do receive #{}", idx);
                readTop = channel.receive();
            }
            if (readTop == null) {
                LOG.info("eof {}", idx);
                return -1;
            }

            final int result = reader.applyAsInt(readTop);
            LOG.debug("read0 #{} = {}", idx, result);
            if (!readTop.buf.hasRemaining()) {
                LOG.debug("callback {} run", readTop.callback);
                readTop.callback.run();
                readTop = null;
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(e.getMessage());
        } catch (SuspendExecution e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    static class Chunk {
        final ByteBuffer buf;
        final Runnable callback;

        Chunk(ByteBuffer buf, Runnable callback) {
            this.buf = buf;
            this.callback = callback;
        }
    }
}
