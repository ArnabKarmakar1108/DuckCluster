package io.duckcluster.worker.shuffle;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Long-lived gRPC channels to other workers, used for worker-to-worker shuffle transfers. */
final class PeerChannels implements AutoCloseable {
    private final int maxInboundMessageBytes;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    PeerChannels(int maxInboundMessageBytes) {
        this.maxInboundMessageBytes = maxInboundMessageBytes;
    }

    ManagedChannel channel(String host, int port) {
        return channels.computeIfAbsent(host + ":" + port, ignored -> ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .maxInboundMessageSize(maxInboundMessageBytes)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build());
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
    }
}
