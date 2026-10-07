CREATE TABLE IF NOT EXISTS mapa_chamados_geocode (
    id INT AUTO_INCREMENT PRIMARY KEY,
    contrato_id VARCHAR(40) NOT NULL,
    cliente_id VARCHAR(40) NULL,
    address_hash CHAR(64) NOT NULL,
    latitude DECIMAL(10,7) NOT NULL,
    longitude DECIMAL(10,7) NOT NULL,
    provider VARCHAR(40) NOT NULL DEFAULT 'sgp',
    geocoded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_mapa_chamados_geocode_contrato (contrato_id),
    INDEX idx_mapa_chamados_geocode_hash (address_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
