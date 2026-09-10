ALTER TABLE fleetq_result_deliveries
    MODIFY status ENUM('pending', 'sent', 'delivered', 'unknown') NOT NULL;

UPDATE fleetq_result_deliveries
SET status = 'delivered'
WHERE status = 'sent';

ALTER TABLE fleetq_result_deliveries
    MODIFY status ENUM('pending', 'delivered', 'unknown') NOT NULL;
