-- Creates the staging table for raw flight price data from CSV

USE staging;

-- Drop table if exists for clean initialization
DROP TABLE IF EXISTS flight_staging;

-- Create staging table matching the CSV structure (18 columns)
CREATE TABLE flight_staging (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source VARCHAR(10),
    source_name VARCHAR(200),
    destination VARCHAR(10),
    destination_name VARCHAR(200),
    departure_datetime VARCHAR(50),
    arrival_datetime VARCHAR(50),
    duration_hrs DECIMAL(10, 2),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(15, 2),
    tax_surcharge_bdt DECIMAL(15, 2),
    total_fare_bdt DECIMAL(15, 2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    -- Validation flags
    is_validated BOOLEAN DEFAULT FALSE,
    validation_errors TEXT,
    validated_at TIMESTAMP NULL,
    -- Ingestion metadata
    ingestion_run_id VARCHAR(255),
    ingested_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create index for common queries
CREATE INDEX idx_airline ON flight_staging(airline);
CREATE INDEX idx_source_dest ON flight_staging(source, destination);
CREATE INDEX idx_seasonality ON flight_staging(seasonality);

-- Grant permissions
GRANT ALL PRIVILEGES ON staging.* TO 'airflow'@'%';
FLUSH PRIVILEGES;
