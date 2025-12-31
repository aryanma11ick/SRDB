BEGIN;

-- 1) Suppliers table
CREATE TABLE IF NOT EXISTS suppliers (
    id SERIAL PRIMARY KEY,
    supplier_code TEXT UNIQUE,
    name TEXT,
    email_domain TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Ensure a default "unknown" supplier exists
INSERT INTO suppliers (supplier_code, name)
VALUES ('unknown', 'Unknown Supplier')
ON CONFLICT (supplier_code) DO NOTHING;

-- 2) Add supplier_id to emails and dispute_documents, backfill to "unknown"
DO $$
DECLARE
    sid INTEGER;
BEGIN
    SELECT id INTO sid FROM suppliers WHERE supplier_code = 'unknown';
    IF sid IS NULL THEN
        RAISE EXCEPTION 'Default supplier not found; aborting migration.';
    END IF;

    ALTER TABLE emails ADD COLUMN IF NOT EXISTS supplier_id INTEGER;
    ALTER TABLE dispute_documents ADD COLUMN IF NOT EXISTS supplier_id INTEGER;

    UPDATE emails SET supplier_id = COALESCE(supplier_id, sid);
    UPDATE dispute_documents SET supplier_id = COALESCE(supplier_id, sid);

    EXECUTE format('ALTER TABLE emails ALTER COLUMN supplier_id SET DEFAULT %s', sid);
    EXECUTE format('ALTER TABLE dispute_documents ALTER COLUMN supplier_id SET DEFAULT %s', sid);

    ALTER TABLE emails ALTER COLUMN supplier_id SET NOT NULL;
    ALTER TABLE dispute_documents ALTER COLUMN supplier_id SET NOT NULL;

    BEGIN
        ALTER TABLE emails ADD CONSTRAINT emails_supplier_id_fkey
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id);
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;

    BEGIN
        ALTER TABLE dispute_documents ADD CONSTRAINT dispute_documents_supplier_id_fkey
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id);
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
END $$;

COMMIT;
