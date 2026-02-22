package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ConcatenateMergeStrategy implements MergeStrategy {
    @Override
    public MergeStrategyType type() {
        return MergeStrategyType.CONCATENATE;
    }

    @Override
    public QueryResult merge(MergeContext context) {
        List<String> columns = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();
        Map<String, Long> workerDurationsMs = new LinkedHashMap<>();

        for (var fragment : context.fragmentResults()) {
            workerDurationsMs.put(fragment.workerId(), fragment.durationMs());
            for (RowBatchData batch : fragment.batches()) {
                if (columns.isEmpty()) {
                    columns.addAll(batch.columnNames());
                }
                for (List<String> row : batch.rows()) {
                    rows.add(new ArrayList<>(row));
                }
            }
        }

        QueryResult.QueryStats stats = new QueryResult.QueryStats(
                type(),
                workerDurationsMs.size(),
                context.fragmentResults().size(),
                context.durationMs(),
                workerDurationsMs);
        return new QueryResult(context.queryId(), columns, rows, stats);
    }
}
