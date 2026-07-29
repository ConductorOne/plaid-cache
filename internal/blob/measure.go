// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package blob

import (
	"fmt"
	"os"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// Measure reports what a published body costs now, and whether that figure can
// be believed.
//
// A body written within the settle window still reports a provisional cost, so a
// caller correcting its accounting can tell the difference between "this is what
// it costs" and "ask again shortly".
func (s *Store) Measure(id ids.OutputID, now time.Time) (bytes int64, settled bool, err error) {
	fi, err := os.Stat(s.Path(id))
	if err != nil {
		return 0, false, fmt.Errorf("Measure: %w", err)
	}
	b, ok := settledBytes(fi, now)
	return b, ok, nil
}
