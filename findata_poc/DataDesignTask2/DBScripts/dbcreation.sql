-- Create Schema if it doesn't exist (PostgreSQL)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'financial_data') THEN
        CREATE SCHEMA financial_data;
    END IF;
END $$;

-- Create the 'appuser' user if it doesn't exist (PostgreSQL)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'appuser') THEN
        CREATE USER appuser WITH PASSWORD 'your_password'; --TODO:: Need to change the password!
    END IF;
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'user appuser already exists, skipping';
END$$;

-- Grant USAGE on schema 'financial_data' to 'appuser' (Idempotent)
DO $$
BEGIN
  GRANT USAGE ON SCHEMA financial_data TO appuser;
EXCEPTION
    WHEN invalid_grant_operation THEN
        RAISE NOTICE 'USAGE privilege already granted on schema financial_data to appuser, skipping';
END $$;


-- Clients Table (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'financial_data' AND tablename = 'clients') THEN
        CREATE TABLE financial_data.Clients (
            client_id VARCHAR(50) NOT NULL,
            partner_id VARCHAR(50) NOT NULL,
            data_date DATE NOT NULL,
            name VARCHAR(255),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (client_id, partner_id, data_date)
        );
    END IF;
END $$;

-- Accounts Table (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'financial_data' AND tablename = 'accounts') THEN
        CREATE TABLE financial_data.Accounts (
            account_id VARCHAR(50) NOT NULL,
            client_id VARCHAR(50) NOT NULL,
            partner_id VARCHAR(50) NOT NULL,
            data_date DATE NOT NULL,
            value DECIMAL(20, 2),
            currency VARCHAR(10),
            name VARCHAR(255),
            type VARCHAR(50),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (account_id, partner_id, data_date),
            FOREIGN KEY (client_id, partner_id, data_date) REFERENCES financial_data.Clients (client_id, partner_id, data_date)
        );
    END IF;
END $$;

-- Holdings Table (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'financial_data' AND tablename = 'holdings') THEN
        CREATE TABLE financial_data.Holdings (
            holding_id VARCHAR(50) NOT NULL,
            account_id VARCHAR(50) NOT NULL,
            partner_id VARCHAR(50) NOT NULL,
            data_date DATE NOT NULL,
            name VARCHAR(255),
            security VARCHAR(50) NULL,
            quantity DECIMAL(20, 4),
            buy_price DECIMAL(20, 4),
            is_cash_like BOOLEAN,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (holding_id, partner_id, data_date),
            FOREIGN KEY (account_id, partner_id, data_date) REFERENCES financial_data.Accounts (account_id, partner_id, data_date)
        );
    END IF;
END $$;

-- Transactions Table (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'financial_data' AND tablename = 'transactions') THEN
      CREATE TYPE financial_data.transaction_type AS ENUM ('BUY', 'SELL', 'DIVIDEND', 'DEPOSIT', 'WITHDRAWAL');
        CREATE TABLE financial_data.Transactions (
            transaction_id VARCHAR(50) NOT NULL,
            account_id VARCHAR(50) NOT NULL,
            holding_id VARCHAR(50) NULL,
            partner_id VARCHAR(50) NOT NULL,
            data_date DATE NOT NULL,
            type financial_data.transaction_type,
            quantity DECIMAL(20, 4),
            value DECIMAL(20, 2),
            date DATE,
            settle_date DATE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (transaction_id, partner_id, data_date),
            FOREIGN KEY (account_id, partner_id, data_date) REFERENCES financial_data.Accounts (account_id, partner_id, data_date),
            FOREIGN KEY (holding_id, partner_id, data_date) REFERENCES financial_data.Holdings (holding_id, partner_id, data_date)
        );
    END IF;
END $$;

-- ProcessedFiles Table (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'financial_data' AND tablename = 'processed_files') THEN
        CREATE TABLE financial_data.Processed_Files (
            unique_key VARCHAR(255) PRIMARY KEY, --f"{request_id}-{partner_id}-{client_id}-{datetime.date.today()}"
            request_id VARCHAR(50) NOT NULL,
            partner_id VARCHAR(50) NOT NULL,
            client_id VARCHAR(50) NOT NULL,
            data_date DATE NOT NULL,
            processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    END IF;
END $$;

-- FileProcessingStatus Table (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'financial_data' AND tablename = 'file_processing_status') THEN
        CREATE TABLE financial_data.File_Processing_Status (
            message_id UUID PRIMARY KEY,  -- Use UUID for message_id
            file_path VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL,
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    END IF;
END $$;

-- Composite Type for Clients (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'client_type') THEN
        CREATE TYPE financial_data.client_type AS (
            client_id VARCHAR(50),
            partner_id VARCHAR(50),
            data_date DATE,
            name VARCHAR(255),
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE
        );
    END IF;
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'type client_type already exists, skipping';
END$$;

-- Composite Type for Accounts (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'account_type') THEN
        CREATE TYPE financial_data.account_type AS (
            account_id VARCHAR(50),
            client_id VARCHAR(50),
            partner_id VARCHAR(50),
            data_date DATE,
            value DECIMAL(20, 2),
            currency VARCHAR(10),
            name VARCHAR(255),
            type VARCHAR(50)
        );
    END IF;
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'type account_type already exists, skipping';
END$$;

-- Composite Type for Holdings (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'holding_type') THEN
        CREATE TYPE financial_data.holding_type AS (
            holding_id VARCHAR(50),
            account_id VARCHAR(50),
            partner_id VARCHAR(50),
            data_date DATE,
            name VARCHAR(255),
            security VARCHAR(50),
            quantity DECIMAL(20, 4),
            buy_price DECIMAL(20, 4),
            is_cash_like BOOLEAN
        );
    END IF;
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'type holding_type already exists, skipping';
END$$;

-- Composite Type for Transactions (Idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_type') THEN
        CREATE TYPE financial_data.transaction_type AS (
            transaction_id VARCHAR(50),
            account_id VARCHAR(50),
            holding_id VARCHAR(50),
            partner_id VARCHAR(50),
            data_date DATE,
            type VARCHAR(20),
            quantity DECIMAL(20, 4),
            value DECIMAL(20, 2),
            date DATE,
            settle_date DATE
        );
    END IF;
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'type transaction_type already exists, skipping';
END$$;

-- Grant SELECT, INSERT, UPDATE, DELETE on all TABLES in schema 'financial_data' to 'appuser' (Idempotent)
DO $$
BEGIN
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA financial_data TO appuser;
EXCEPTION
    WHEN invalid_grant_operation THEN
        RAISE NOTICE 'SELECT, INSERT, UPDATE, DELETE privileges already granted on all tables in schema financial_data to appuser, skipping';
END $$;
