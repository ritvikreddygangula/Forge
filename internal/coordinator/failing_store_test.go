package coordinator_test

import (
	"errors"

	"github.com/ritvikreddygangula/forge/internal/job"
)

// errStoreFailure is a sentinel error used by failingStore to simulate a
// store failure that is NOT job.ErrNotFound, so handlers must be exercised
// on their generic-error (500) branch rather than their not-found (404)
// branch.
var errStoreFailure = errors.New("simulated store failure")

// failingStore is a test-only job.Store implementation whose methods all
// return a non-ErrNotFound error, used to prove that handlers correctly
// distinguish "not found" from other store failures.
type failingStore struct{}

func (failingStore) Create(image string, command []string, timeoutSeconds int) (*job.Job, error) {
	return nil, errStoreFailure
}

func (failingStore) Get(id string) (*job.Job, error) {
	return nil, errStoreFailure
}

func (failingStore) ClaimNext(workerID string) (*job.Job, error) {
	return nil, errStoreFailure
}

func (failingStore) Complete(id string, status job.Status, stdout, stderr string, exitCode int) error {
	return errStoreFailure
}

func (failingStore) RequeueRunning(workerID string) ([]*job.Job, error) {
	return nil, errStoreFailure
}
