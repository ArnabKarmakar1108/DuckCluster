package io.duckcluster.coordinator.worker;

import io.duckcluster.common.registry.WorkerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class WorkerChannelPool implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerChannelPool.class);

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final int maxInboundMessageBytes;

    public WorkerChannelPool() {
        this(32 * 1024 * 1024);
    }

    public WorkerChannelPool(int maxInboundMessageBytes) {
        this.maxInboundMessageBytes = maxInboundMessageBytes;
    }

    public ManagedChannel getChannel(WorkerRegistry.WorkerRecord worker) {
        return channels.computeIfAbsent(worker.workerId(), id ->
                ManagedChannelBuilder
                        .forAddress(worker.host(), worker.port())
                        .usePlaintext()
                        .maxInboundMessageSize(maxInboundMessageBytes)
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .keepAliveWithoutCalls(true)
                        .build());
    }

    public void removeChannel(String workerId) {
        ManagedChannel channel = channels.remove(workerId);
        if (channel != null) {
            channel.shutdown();
        }
    }

    @Override
    public void close() {
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            ManagedChannel channel = entry.getValue();
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
    }
}
