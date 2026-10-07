CREATE TABLE IF NOT EXISTS agenda_tecnicos_almocos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tecnico_id VARCHAR(80) NOT NULL,
    data DATE NOT NULL,
    hora_inicio TIME NOT NULL,
    hora_fim TIME NOT NULL,
    criado_por VARCHAR(120) NULL,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_almoco_tecnico_data (tecnico_id, data),
    INDEX idx_almoco_data (data)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Permissao usada pela API:
-- agenda_tecnicos_gerenciar_almoco
