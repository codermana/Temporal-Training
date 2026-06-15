package saga

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// A travel-booking saga: book a flight, a hotel, then rent a car. If any later
// step fails, cancel the bookings already made — in reverse — so the traveler is
// never left with a partial, charged itinerary. Compensation-slice pattern.
func TravelBookingSaga(ctx workflow.Context, tripID string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	var compensations []func()
	compensate := func() {
		for i := len(compensations) - 1; i >= 0; i-- {
			compensations[i]()
		}
	}

	var flight string
	if err := workflow.ExecuteActivity(ctx, "BookFlight", tripID).Get(ctx, &flight); err != nil {
		return "CANCELLED", err
	}
	compensations = append(compensations, func() {
		_ = workflow.ExecuteActivity(ctx, "CancelFlight", flight).Get(ctx, nil)
	})

	var hotel string
	if err := workflow.ExecuteActivity(ctx, "BookHotel", tripID).Get(ctx, &hotel); err != nil {
		compensate()
		return "CANCELLED", nil
	}
	compensations = append(compensations, func() {
		_ = workflow.ExecuteActivity(ctx, "CancelHotel", hotel).Get(ctx, nil)
	})

	var car string
	if err := workflow.ExecuteActivity(ctx, "RentCar", tripID).Get(ctx, &car); err != nil {
		compensate()
		return "CANCELLED", nil
	}

	return "BOOKED " + flight + " " + hotel + " " + car, nil
}
