// A travel-booking saga: reserve a flight, a hotel, then a car. If any later
// step fails, cancel the bookings already made — in reverse — so the traveler is
// never left with a partial, charged itinerary. The Java SDK's Saga helper holds
// the compensation stack and unwinds it for you on compensate().
@ActivityInterface
interface TravelActivities {
  String bookFlight(String tripId);

  String bookHotel(String tripId);

  String rentCar(String tripId);

  void cancelFlight(String confirmation);

  void cancelHotel(String confirmation);
}

class TravelBookingSaga {
  private final TravelActivities activities =
      Workflow.newActivityStub(
          TravelActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(30)).build());

  String book(String tripId) {
    Saga saga = new Saga(new Saga.Options.Builder().build());
    try {
      String flight = activities.bookFlight(tripId);
      saga.addCompensation(activities::cancelFlight, flight);

      String hotel = activities.bookHotel(tripId);
      saga.addCompensation(activities::cancelHotel, hotel);

      String car = activities.rentCar(tripId); // fails for "nocar" trips
      return "BOOKED " + flight + " " + hotel + " " + car;
    } catch (ActivityFailure e) {
      saga.compensate(); // cancels hotel then flight, in reverse order
      return "CANCELLED";
    }
  }
}
