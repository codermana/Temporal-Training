package training.temporal.aws;

/** A parsed {@code s3://bucket/key} reference — the unit Activities pass forward (never bytes). */
public record S3Uri(String bucket, String key) {

  public static S3Uri parse(String uri) {
    if (uri == null || !uri.startsWith("s3://")) {
      throw new IllegalArgumentException("not an s3:// URI: " + uri);
    }
    String rest = uri.substring("s3://".length());
    int slash = rest.indexOf('/');
    if (slash < 0) {
      throw new IllegalArgumentException("s3 URI has no key: " + uri);
    }
    return new S3Uri(rest.substring(0, slash), rest.substring(slash + 1));
  }

  public static String of(String bucket, String key) {
    return "s3://" + bucket + "/" + key;
  }

  @Override
  public String toString() {
    return of(bucket, key);
  }
}
