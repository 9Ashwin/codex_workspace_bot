package storage

import (
	"context"
	"fmt"
)

func (s *Store) BeginFleetQResult(ctx context.Context, requestID, resultID, appID, receiveID, receiveType, content string) (bool, error) {
	if requestID == "" || resultID == "" || appID == "" || receiveID == "" || receiveType == "" {
		return false, fmt.Errorf("fleetq result identity is required")
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO fleetq_result_deliveries (request_id,result_id,app_id,receive_id,receive_type,content,status) VALUES (?,?,?,?,?,?,'pending') ON DUPLICATE KEY UPDATE request_id=request_id`, requestID, resultID, appID, receiveID, receiveType, content)
	if err != nil {
		return false, fmt.Errorf("begin fleetq result: %w", err)
	}
	var status string
	if err := s.DB.QueryRowContext(ctx, `SELECT status FROM fleetq_result_deliveries WHERE request_id=?`, requestID).Scan(&status); err != nil {
		return false, fmt.Errorf("read fleetq result status: %w", err)
	}
	return status == "pending", nil
}

func (s *Store) MarkFleetQResultSent(ctx context.Context, requestID string) error {
	if _, err := s.DB.ExecContext(ctx, `UPDATE fleetq_result_deliveries SET status='sent', error_reason=NULL, sent_at=CURRENT_TIMESTAMP(3), updated_at=CURRENT_TIMESTAMP(3) WHERE request_id=? AND status='pending'`, requestID); err != nil {
		return fmt.Errorf("mark fleetq result sent: %w", err)
	}
	return nil
}

func (s *Store) MarkFleetQResultUnknown(ctx context.Context, requestID, reason string) error {
	if _, err := s.DB.ExecContext(ctx, `UPDATE fleetq_result_deliveries SET status='unknown', error_reason=?, updated_at=CURRENT_TIMESTAMP(3) WHERE request_id=? AND status='pending'`, reason, requestID); err != nil {
		return fmt.Errorf("mark fleetq result unknown: %w", err)
	}
	return nil
}
