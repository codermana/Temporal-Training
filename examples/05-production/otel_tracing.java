// OpenTelemetry/OpenTracing span propagation: spans started on the client flow
// through Workflow and Activity executions, so one trace spans the whole run.
// Needs io.temporal:temporal-opentracing on the classpath.
class TracingSetup {
  WorkflowClient client(WorkflowServiceStubs service) {
    OpenTracingOptions otOptions =
        OpenTracingOptions.newBuilder().setTracer(GlobalTracer.get()).build();

    return WorkflowClient.newInstance(
        service,
        WorkflowClientOptions.newBuilder()
            .setInterceptors(new OpenTracingClientInterceptor(otOptions))
            .build());
  }

  WorkerFactory factory(WorkflowClient client) {
    // The matching Worker-side interceptor continues the trace into Activities.
    return WorkerFactory.newInstance(
        client,
        WorkerFactoryOptions.newBuilder()
            .setWorkerInterceptors(new OpenTracingWorkerInterceptor())
            .build());
  }
}
