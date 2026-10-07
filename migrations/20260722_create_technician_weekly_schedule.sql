CREATE TABLE IF NOT EXISTS schedule_shifts (
    id VARCHAR(40) NOT NULL PRIMARY KEY,
    name VARCHAR(80) NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    sort_order INT NOT NULL DEFAULT 0,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_schedule_shifts_active_order (active, sort_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS technician_rotation_groups (
    id VARCHAR(40) NOT NULL PRIMARY KEY,
    name VARCHAR(80) NOT NULL,
    sort_order INT NOT NULL DEFAULT 0,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_rotation_groups_active_order (active, sort_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS technician_group_assignments (
    id VARCHAR(40) NOT NULL PRIMARY KEY,
    technician_id VARCHAR(80) NOT NULL,
    rotation_group_id VARCHAR(40) NOT NULL,
    valid_from_week DATE NOT NULL,
    valid_until_week DATE NULL,
    sort_order INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_assignment_technician_from (technician_id, valid_from_week),
    INDEX idx_assignment_lookup (technician_id, valid_from_week, valid_until_week),
    INDEX idx_assignment_group (rotation_group_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS rotation_group_shift_rules (
    id VARCHAR(40) NOT NULL PRIMARY KEY,
    rotation_group_id VARCHAR(40) NOT NULL,
    cycle_position INT NOT NULL,
    shift_id VARCHAR(40) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_rule_group_cycle (rotation_group_id, cycle_position),
    INDEX idx_rule_shift (shift_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS weekly_technician_schedule_overrides (
    id VARCHAR(40) NOT NULL PRIMARY KEY,
    week_start DATE NOT NULL,
    technician_id VARCHAR(80) NOT NULL,
    shift_id VARCHAR(40) NOT NULL,
    rotation_group_id VARCHAR(40) NULL,
    sort_order INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_override_week_technician (week_start, technician_id),
    INDEX idx_override_shift (week_start, shift_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE weekly_technician_schedule_overrides
    ADD COLUMN IF NOT EXISTS rotation_group_id VARCHAR(40) NULL AFTER shift_id;

INSERT IGNORE INTO schedule_shifts (id, name, start_time, end_time, sort_order, active)
VALUES
    ('turno_1', 'Turno 1', '08:00:00', '16:48:00', 1, 1),
    ('turno_2', 'Turno 2', '09:00:00', '19:00:00', 2, 1);

INSERT IGNORE INTO technician_rotation_groups (id, name, sort_order, active)
VALUES
    ('grupo_1', 'Grupo 1', 1, 1),
    ('grupo_2', 'Grupo 2', 2, 1);

INSERT IGNORE INTO technician_group_assignments
    (id, technician_id, rotation_group_id, valid_from_week, valid_until_week, sort_order)
VALUES
    ('atribuicao_inicial_g1_1', 'Alex', 'grupo_1', '2026-06-22', NULL, 1),
    ('atribuicao_inicial_g1_2', 'Claudinei', 'grupo_1', '2026-06-22', NULL, 2),
    ('atribuicao_inicial_g1_3', 'Nelson', 'grupo_1', '2026-06-22', NULL, 3),
    ('atribuicao_inicial_g1_4', 'Ramon', 'grupo_1', '2026-06-22', NULL, 4),
    ('atribuicao_inicial_g2_1', 'Alan', 'grupo_2', '2026-06-22', NULL, 1),
    ('atribuicao_inicial_g2_2', 'Paulino', 'grupo_2', '2026-06-22', NULL, 2),
    ('atribuicao_inicial_g2_3', 'Emerson', 'grupo_2', '2026-06-22', NULL, 3),
    ('atribuicao_inicial_g2_4', 'Lucio', 'grupo_2', '2026-06-22', NULL, 4);

INSERT IGNORE INTO rotation_group_shift_rules
    (id, rotation_group_id, cycle_position, shift_id)
VALUES
    ('regra_g1_0', 'grupo_1', 0, 'turno_1'),
    ('regra_g1_1', 'grupo_1', 1, 'turno_2'),
    ('regra_g2_0', 'grupo_2', 0, 'turno_2'),
    ('regra_g2_1', 'grupo_2', 1, 'turno_1');
