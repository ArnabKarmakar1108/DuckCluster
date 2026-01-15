package io.duckcluster.common.registry;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class WorkerRegistry {
    public enum WorkerStatus {
        HEALTHY,
        UNHEALTHY
    }

    public record WorkerRecord(
            String workerId,
            String host,
            int port,
            int numThreads,
            WorkerStatus status,
            Instant registeredAt,
            Instant lastHeartbeatAt,
            double load) {}

    private final Map<String, WorkerRecord> workers = new ConcurrentHashMap<>();

    public WorkerRecord register(String workerId, String host, int port, int numThreads) {
        Instant now = Instant.now();
        WorkerRecord record = new WorkerRecord(
                workerId, host, port, numThreads, WorkerStatus.HEALTHY, now, now, 0.0);
        workers.put(workerId, record);
        return record;
    }

    public Optional<WorkerRecord> heartbeat(String workerId, double load) {
        WorkerRecord existing = workers.get(workerId);
        if (existing == null) {
            return Optional.empty();
        }
        WorkerRecord updated = new WorkerRecord(
                existing.workerId(),
                existing.host(),
                existing.port(),
                existing.numThreads(),
                WorkerStatus.HEALTHY,
                existing.registeredAt(),
                Instant.now(),
                load);
        workers.put(workerId, updated);
        return Optional.of(updated);
    }

    public List<WorkerRecord> listWorkers() {
        return new ArrayList<>(workers.values());
    }

    public int workerCount() {
        return workers.size();
    }

    public boolean isHealthy() {
        return !workers.isEmpty()
                && workers.values().stream().allMatch(w -> w.status() == WorkerStatus.HEALTHY);
    }
}
