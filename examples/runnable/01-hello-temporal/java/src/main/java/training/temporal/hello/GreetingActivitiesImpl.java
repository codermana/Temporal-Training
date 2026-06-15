package training.temporal.hello;

public class GreetingActivitiesImpl implements GreetingActivities {
  @Override
  public String composeGreeting(String name) {
    // sleep 10
    return "Hello, " + name + " from a Temporal Activity";
  }
}
