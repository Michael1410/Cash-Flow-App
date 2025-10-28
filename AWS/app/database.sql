CREATE TABLE IF NOT EXISTS financial_statements (
  id              BIGSERIAL PRIMARY KEY,
  company         TEXT NOT NULL,
  fiscal_year     INT  NOT NULL,
  period_end      DATE,
  currency        TEXT,
  source_filename TEXT,
  payload         JSONB NOT NULL,        -- normalized, config-driven JSON
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (company, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_fs_company ON financial_statements(company);
CREATE INDEX IF NOT EXISTS idx_fs_year    ON financial_statements(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_fs_payload_gin ON financial_statements USING GIN (payload);