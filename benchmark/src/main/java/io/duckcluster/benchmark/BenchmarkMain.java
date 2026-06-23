package io.duckcluster.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class BenchmarkMain {

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.parse(args);
        QueryCatalog catalog = new QueryCatalog(config.queriesDir());
        BenchmarkRunner runner = new BenchmarkRunner(config, catalog);
        Map<String, Object> results = runner.run();

        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        Path output = config.outputPath();
        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }
        mapper.writeValue(output.toFile(), results);
        System.out.println("Wrote benchmark results to " + output.toAbsolutePath());
    }
}
