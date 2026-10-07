ALTER TABLE schedule_shifts
    ADD COLUMN IF NOT EXISTS tipo_turno VARCHAR(20) NOT NULL DEFAULT 'alternancia' AFTER active;

UPDATE schedule_shifts
SET tipo_turno = 'alternancia'
WHERE tipo_turno IS NULL OR tipo_turno NOT IN ('alternancia', 'fixo');

CREATE TABLE IF NOT EXISTS fixed_shift_technician_assignments (
    id VARCHAR(40) NOT NULL PRIMARY KEY,
    shift_id VARCHAR(40) NOT NULL,
    technician_id VARCHAR(80) NOT NULL,
    sort_order INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_fixed_shift_technician (shift_id, technician_id),
    INDEX idx_fixed_shift_order (shift_id, sort_order),
    INDEX idx_fixed_technician (technician_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
