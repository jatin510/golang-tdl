package api

import (
	"context"
	"sync"
)

type SpreadsheetsMock struct {
	mock sync.Mutex

	Rows map[string][][]string
}

func (s *SpreadsheetsMock) AppendRow(ctx context.Context, sheetName string, row []string) error {
	s.mock.Lock()
	defer s.mock.Unlock()

	if s.Rows == nil {
		s.Rows = make(map[string][][]string)
	}

	s.Rows[sheetName] = append(s.Rows[sheetName], row)

	return nil
}
