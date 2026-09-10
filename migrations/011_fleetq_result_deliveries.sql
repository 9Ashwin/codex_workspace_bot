CREATE TABLE IF NOT EXISTS fleetq_result_deliveries (
    request_id VARCHAR(191) PRIMARY KEY,
    result_id VARCHAR(255) NOT NULL,
    app_id VARCHAR(191) NOT NULL,
    receive_id VARCHAR(191) NOT NULL,
    receive_type VARCHAR(32) NOT NULL,
    content MEDIUMTEXT NOT NULL,
    status ENUM('pending', 'sent', 'unknown') NOT NULL,
    error_reason VARCHAR(512) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    sent_at DATETIME(3) NULL,
    KEY ix_fleetq_result_deliveries_status (status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
