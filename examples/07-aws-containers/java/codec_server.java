// A PayloadCodec encrypts every payload before it leaves the client/Worker, so the
// Temporal server only ever stores ciphertext. A standalone "codec server" runs the
// reverse so the Web UI / CLI can decode on demand (with auth) - useful when history
// holds S3 URIs or other sensitive references.
class EncryptionCodec implements PayloadCodec {
  @Override
  public List<Payload> encode(List<Payload> payloads) {
    return payloads.stream().map(this::encrypt).toList(); // e.g. AES-GCM
  }

  @Override
  public List<Payload> decode(List<Payload> payloads) {
    return payloads.stream().map(this::decrypt).toList();
  }
}

class CodecWiring {
  WorkflowClient client(WorkflowServiceStubs service) {
    DataConverter converter =
        new CodecDataConverter(
            DefaultDataConverter.newDefaultInstance(), List.of(new EncryptionCodec()));

    return WorkflowClient.newInstance(
        service, WorkflowClientOptions.newBuilder().setDataConverter(converter).build());
  }
}
