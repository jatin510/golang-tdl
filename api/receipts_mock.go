package api

import (
	"context"
	"sync"
	"tickets/entities"
	"time"

	"github.com/google/uuid"
)

type ReceiptsMock struct {
	mock sync.Mutex

	IssuedReceipts []entities.IssueReceiptRequest
}

func (r *ReceiptsMock) IssueReceipt(ctx context.Context, request entities.IssueReceiptRequest) (entities.IssueReceiptResponse, error) {
	r.mock.Lock()
	defer r.mock.Unlock()

	r.IssuedReceipts = append(r.IssuedReceipts, request)

	return entities.IssueReceiptResponse{
		ReceiptNumber: uuid.NewString(),
		IssuedAt:      time.Now(),
	}, nil
}
