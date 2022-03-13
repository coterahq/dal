package e2e

import (
	"os"
	"testing"
)

func SkipE2E(t *testing.T) {
	if os.Getenv("TEST_E2E") == "" {
		t.Skip("Set the TEST_E2E environment variable to run the E2E tests.")
	}
}
