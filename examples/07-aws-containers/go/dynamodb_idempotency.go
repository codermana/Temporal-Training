// Activities are at-least-once: a retried or timed-out Activity can run its side
// effect twice. For non-idempotent externals (charge a card, POST to a partner),
// guard with a DynamoDB conditional write keyed by the Activity's idempotency key
// (e.g. the Workflow ID + step). The conditional put is the dedupe; the partner
// call only runs on the first writer.
//
// The DynamoDB calls sit behind an IdempotencyStore interface so the Temporal
// usage compiles without the AWS SDK.
package aws

import "context"

// ErrDuplicateKey is returned by Claim when the idempotency key already exists.
var ErrDuplicateKey = errDuplicateKey{}

type errDuplicateKey struct{}

func (errDuplicateKey) Error() string { return "idempotency key already claimed" }

// IdempotencyStore is the slice of DynamoDB this Activity needs. Claim is a
// conditional put (attribute_not_exists(pk)); Lookup/Store read and write the
// memoized result.
type IdempotencyStore interface {
	Claim(ctx context.Context, key string) error // returns ErrDuplicateKey if seen
	Lookup(ctx context.Context, key string) (result string, err error)
	Store(ctx context.Context, key, result string) error
}

// IdempotencyActivities wraps the store plus the non-idempotent partner call.
type IdempotencyActivities struct {
	Store      IdempotencyStore
	PayPartner func(ctx context.Context, amountCents int64) (string, error)
}

// ChargeOnce runs the side effect at most once per idempotency key.
func (a *IdempotencyActivities) ChargeOnce(ctx context.Context, idempotencyKey string, amountCents int64) (string, error) {
	if err := a.Store.Claim(ctx, idempotencyKey); err != nil {
		if err == ErrDuplicateKey {
			// A previous attempt already claimed this key — return the memoized result.
			return a.Store.Lookup(ctx, idempotencyKey)
		}
		return "", err
	}

	result, err := a.PayPartner(ctx, amountCents) // the non-idempotent side effect
	if err != nil {
		return "", err
	}
	if err := a.Store.Store(ctx, idempotencyKey, result); err != nil {
		return "", err
	}
	return result, nil
}
