package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

func evalWithVerify(ctx context.Context, e Eval) error {
	results, err := e.Eval(ctx)
	if err != nil {
		return fmt.Errorf("Unable to evaluate rule '%s': %v", e.String(), err)
	}
	for _, result := range results {
		if result.Success || result.IsAbsent {
			return fmt.Errorf("Stop by matched rule '%s', result '%s': %f (time %d), absent: %v", e.String(), result.Name, result.V, result.T, result.IsAbsent)
		}
	}

	return nil
}

func autostopChecker(autostopChecks []Eval) {
	ctx := context.Background()
	timer := time.NewTicker(time.Minute)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			if atomic.LoadInt32(&running) == 0 {
				break
			}
			for _, e := range autostopChecks {
				if err := evalWithVerify(ctx, e); err != nil {
					log.Printf("%v", err)
					atomic.StoreInt32(&running, 0)
					break
				}
			}
		}
	}
}
