ALTER TABLE agenda_tecnicos_almocos
    ADD COLUMN IF NOT EXISTS tipo VARCHAR(32) NOT NULL DEFAULT 'almoco' AFTER data,
    ADD COLUMN IF NOT EXISTS descricao VARCHAR(255) NULL AFTER tipo;

UPDATE agenda_tecnicos_almocos
SET tipo = 'almoco'
WHERE tipo IS NULL OR tipo = '';
