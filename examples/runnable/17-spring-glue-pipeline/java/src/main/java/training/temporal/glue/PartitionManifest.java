package training.temporal.glue;

/** Result of validating a raw S3 partition: how many Parquet files and how big. */
public record PartitionManifest(int fileCount, long totalBytes, String sampleKey) {}
