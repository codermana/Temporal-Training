from datetime import timedelta

from temporalio import activity, workflow

# A travel-booking saga: reserve a flight, a hotel, then a car. If any later step
# fails, cancel the bookings already made — in reverse — so the traveler is never
# left with a partial, charged itinerary. Manual compensation stack (Python has no
# built-in Saga helper).


@activity.defn
async def book_flight(trip_id: str) -> str:
    return f"flight-{trip_id}"


@activity.defn
async def book_hotel(trip_id: str) -> str:
    return f"hotel-{trip_id}"


@activity.defn
async def rent_car(trip_id: str) -> str:
    if "nocar" in trip_id.lower():
        raise RuntimeError("no rental cars available")
    return f"car-{trip_id}"


@activity.defn
async def cancel_flight(confirmation: str) -> None:
    activity.logger.info("cancelled %s", confirmation)


@activity.defn
async def cancel_hotel(confirmation: str) -> None:
    activity.logger.info("cancelled %s", confirmation)


@workflow.defn
class TravelBookingSaga:
    @workflow.run
    async def book(self, trip_id: str) -> str:
        opts = dict(start_to_close_timeout=timedelta(seconds=30))
        compensations: list = []
        try:
            flight = await workflow.execute_activity(book_flight, trip_id, **opts)
            compensations.append((cancel_flight, flight))

            hotel = await workflow.execute_activity(book_hotel, trip_id, **opts)
            compensations.append((cancel_hotel, hotel))

            car = await workflow.execute_activity(rent_car, trip_id, **opts)
            return f"BOOKED {flight} {hotel} {car}"
        except Exception:
            for compensate, arg in reversed(compensations):
                await workflow.execute_activity(compensate, arg, **opts)
            return "CANCELLED"
