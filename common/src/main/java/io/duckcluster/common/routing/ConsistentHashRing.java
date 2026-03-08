package io.duckcluster.common.routing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class ConsistentHashRing {
    private final TreeMap<Integer, String> ring = new TreeMap<>();
    private final Map<String, List<Integer>> workerPositions = new HashMap<>();
    private final int vnodesPerWorker;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsistentHashRing(int vnodesPerWorker) {
        this.vnodesPerWorker = vnodesPerWorker;
    }

    public void addWorker(String workerId) {
        lock.writeLock().lock();
        try {
            if (workerPositions.containsKey(workerId)) {
                return;
            }
            List<Integer> positions = new ArrayList<>(vnodesPerWorker);
            for (int i = 0; i < vnodesPerWorker; i++) {
                int hash = hash(workerId + "#" + i);
                ring.put(hash, workerId);
                positions.add(hash);
            }
            workerPositions.put(workerId, positions);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeWorker(String workerId) {
        lock.writeLock().lock();
        try {
            List<Integer> positions = workerPositions.remove(workerId);
            if (positions != null) {
                for (int pos : positions) {
                    ring.remove(pos);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<String> getOwners(String key, int count) {
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return List.of();
            }
            int maxDistinct = workerPositions.size();
            int target = Math.min(count, maxDistinct);

            List<String> owners = new ArrayList<>(target);
            int hash = hash(key);

            Map.Entry<Integer, String> entry = ring.ceilingEntry(hash);
            if (entry == null) {
                entry = ring.firstEntry();
            }

            Integer startPos = entry.getKey();
            owners.add(entry.getValue());

            Integer current = startPos;
            while (owners.size() < target) {
                current = ring.higherKey(current);
                if (current == null) {
                    current = ring.firstKey();
                }
                if (current.equals(startPos)) {
                    break;
                }
                String worker = ring.get(current);
                if (!owners.contains(worker)) {
                    owners.add(worker);
                }
            }
            return owners;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getWorkerCount() {
        lock.readLock().lock();
        try {
            return workerPositions.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean hasWorker(String workerId) {
        lock.readLock().lock();
        try {
            return workerPositions.containsKey(workerId);
        } finally {
            lock.readLock().unlock();
        }
    }

    static int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            return ((digest[0] & 0xFF) << 24)
                    | ((digest[1] & 0xFF) << 16)
                    | ((digest[2] & 0xFF) << 8)
                    | (digest[3] & 0xFF);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
