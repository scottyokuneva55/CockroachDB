package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	ErrCodeSerialization = "40001"
)

type TxnError struct {
	Code    string
	Message string
}

func (e *TxnError) Error() string {
	return fmt.Sprintf("Error %s: %s", e.Code, e.Message)
}

var (
	ErrNotLeaseHolder = errors.New("NotLeaseHolderError")
	ErrRangeNotFound  = errors.New("RangeNotFoundError")
)

type Transaction struct {
	ID        string
	Epoch     int32
	ReadTs    time.Time
	WriteTs   time.Time
	IsAborted bool
}

type TimestampCache struct {
	sync.Mutex
	lowWater time.Time
}

func (tc *TimestampCache) Initialize(watermark time.Time) {
	c.Lock()
	defer tc.Unlock()
	c.lowWater = watermark
}

type Range struct {
	mu          sync.RWMutex
	RangeID     int
	LeaseHolder string
	TSCache     *TimestampCache
}

type TxnCoordinator struct {
	mu     sync.Mutex
	ranges map[int]*Range
}

func NewTxnCoordinator() *TxnCoordinator {
	return &TxnCoordinator{
		ranges: make(map[int]*Range),
	}
}

func (tc *TxnCoordinator) Send(ctx context.Context, txn *Transaction, rangeID int, req string) error {
	for {
		tc.mu.Lock()
		r, exists := tc.ranges[rangeID]
		tc.mu.Unlock()

		if !exists {
			return ErrRangeNotFound
		}

		r.mu.RLock()
		leaseHolder := r.LeaseHolder
		r.mu.RUnlock()

		err := tc.executeOnLeaseHolder(leaseHolder, txn, r, req)
		if err == nil {
			return nil
		}

		if errors.Is(err, ErrNotLeaseHolder) || errors.Is(err, ErrRangeNotFound) {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		return err
	}
}

func (tc *TxnCoordinator) executeOnLeaseHolder(leaseHolder string, txn *Transaction, r *Range, req string) error {
	time.Sleep(20 * time.Millisecond)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if leaseHolder != r.LeaseHolder {
		return ErrNotLeaseHolder
	}

	r.TSCache.Lock()
	lowWater := r.TSCache.lowWater
	r.TSCache.Unlock()

	if txn.ReadTs.Before(lowWater) {
		if req == "write-conflict" {
			return &TxnError{Code: ErrCodeSerialization, Message: "write-write conflict"}
		}
	}

	return nil
}

func main() {
	fmt.Println("Starting Range Rebalancing Simulation...")

	tc := NewTxnCoordinator()
	tsCache := &TimestampCache{}
	tsCache.Initialize(time.Now().Add(-1 * time.Hour))

	r := &Range{
		RangeID:     1,
		LeaseHolder: "node1",
		TSCache:     tsCache,
	}
	tc.ranges[1] = r

	txn := &Transaction{
		ID:     "txn-1",
		Epoch:  0,
		ReadTs: time.Now(),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		r.mu.Lock()
		r.LeaseHolder = "node2"
		r.TSCache.Initialize(time.Now().Add(-1 * time.Hour))
		r.mu.Unlock()
		fmt.Println("Lease transferred to node2")
	}()

	err := tc.Send(context.Background(), txn, 1, "read")
	wg.Wait()

	if err != nil {
		fmt.Printf("Transaction failed: %v\n", err)
	} else {
		fmt.Printf("Transaction succeeded transparently! Epoch: %d\n", txn.Epoch)
	}
}
