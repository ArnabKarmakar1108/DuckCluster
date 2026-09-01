package io.duckcluster.worker.client;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.proto.v1.CoordinatorServiceGrpc;
import io.duckcluster.proto.v1.RegisterWorkerRequest;
import io.duckcluster.proto.v1.RegisterWorkerResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoordinatorClientReregistrationTest {

    private final AtomicLong bootEpoch = new AtomicLong(1);
    private final AtomicInteger registrations = new AtomicInteger();
    private Server server;
    private CoordinatorClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = ServerBuilder.forPort(0)
                .addService(new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override
                    public void registerWorker(
                            RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> observer) {
                        registrations.incrementAndGet();
                        observer.onNext(RegisterWorkerResponse.newBuilder()
                                .setAccepted(true)
                                .setCoordinatorBootEpoch(bootEpoch.get())
                                .build());
                        observer.onCompleted();
                    }
                })
                .build()
                .start();
        client = new CoordinatorClient(config(server.getPort()));
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.shutdownNow();
    }

    @Test
    void bootEpochChangeReregistersOnceInsteadOfRecursing() {
        AtomicInteger handlerRuns = new AtomicInteger();
        client.setReconnectHandler(() -> {
            handlerRuns.incrementAndGet();
            client.registerWorker("worker-1", "127.0.0.1", 9101, 1, List.of());
        });

        client.registerWorker("worker-1", "127.0.0.1", 9101, 1, List.of());
        bootEpoch.set(2);
        client.registerWorker("worker-1", "127.0.0.1", 9101, 1, List.of());

        assertEquals(1, handlerRuns.get());
        assertEquals(3, registrations.get());
    }

    private static ClusterConfig config(int grpcPort) {
        return new ClusterConfig(
                "127.0.0.1", grpcPort, 8080, Duration.ofSeconds(5), 3, 3, "./data",
                2, 200, 1, 100, 2000, 32, 60_000);
    }
}
