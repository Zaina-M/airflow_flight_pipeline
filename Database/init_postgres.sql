-- Creates the analytics database and KPI tables

-- Create analytics database (if not exists from docker-compose)
SELECT 'CREATE DATABASE analytics'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'analytics')\gexec

-- Connect to analytics database
\c analytics;

-- Create staging table for raw data ingestion
CREATE TABLE IF NOT EXISTS flight_staging (
    airline VARCHAR(100),
    source VARCHAR(10),
    source_name VARCHAR(200),
    destination VARCHAR(10),
    destination_name VARCHAR(200),
    departure_datetime VARCHAR(50),  -- Store as string initially
    arrival_datetime VARCHAR(50),
    duration_hrs VARCHAR(20),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(15, 2),
    tax_surcharge_bdt DECIMAL(15, 2),
    total_fare_bdt DECIMAL(15, 2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    is_validated BOOLEAN DEFAULT FALSE,
    validation_errors TEXT
);

-- Drop existing tables for clean setup
DROP TABLE IF EXISTS flight_data_transformed CASCADE;
DROP TABLE IF EXISTS kpi_avg_fare_by_airline CASCADE;
DROP TABLE IF EXISTS kpi_seasonal_fare_variation CASCADE;
DROP TABLE IF EXISTS kpi_booking_count_by_airline CASCADE;
DROP TABLE IF EXISTS kpi_popular_routes CASCADE;
DROP TABLE IF EXISTS validation_report CASCADE;

-- Transformed flight data table (cleaned and enriched)
CREATE TABLE flight_data_transformed (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source VARCHAR(10) NOT NULL,
    source_name VARCHAR(200),
    destination VARCHAR(10) NOT NULL,
    destination_name VARCHAR(200),
    departure_datetime TIMESTAMP,
    arrival_datetime TIMESTAMP,
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
    -- Enriched columns
    is_peak_season BOOLEAN,
    route VARCHAR(25),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Average Fare by Airline
CREATE TABLE kpi_avg_fare_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    avg_base_fare DECIMAL(15, 2),
    avg_tax_surcharge DECIMAL(15, 2),
    avg_total_fare DECIMAL(15, 2),
    booking_count INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Seasonal Fare Variation
CREATE TABLE kpi_seasonal_fare_variation (
    id SERIAL PRIMARY KEY,
    season_type VARCHAR(20) NOT NULL, -- 'peak' or 'non_peak'
    season_name VARCHAR(50),
    avg_total_fare DECIMAL(15, 2),
    min_fare DECIMAL(15, 2),
    max_fare DECIMAL(15, 2),
    booking_count INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Booking Count by Airline
CREATE TABLE kpi_booking_count_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    total_bookings INT,
    economy_bookings INT,
    business_bookings INT,
    first_class_bookings INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Most Popular Routes
CREATE TABLE kpi_popular_routes (
    id SERIAL PRIMARY KEY,
    source VARCHAR(10) NOT NULL,
    source_name VARCHAR(200),
    destination VARCHAR(10) NOT NULL,
    destination_name VARCHAR(200),
    route VARCHAR(25),
    booking_count INT,
    avg_fare DECIMAL(15, 2),
    rank INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Validation Report Table
CREATE TABLE validation_report (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    total_records INT,
    valid_records INT,
    invalid_records INT,
    missing_values_count INT,
    negative_fares_count INT,
    data_type_errors INT,
    validation_details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_transformed_airline ON flight_data_transformed(airline);
CREATE INDEX idx_transformed_route ON flight_data_transformed(source, destination);
CREATE INDEX idx_transformed_seasonality ON flight_data_transformed(seasonality);
CREATE INDEX idx_transformed_peak ON flight_data_transformed(is_peak_season);

-- Unique constraint for upsert operations (prevents duplicate records)
CREATE UNIQUE INDEX idx_transformed_unique_record 
    ON flight_data_transformed(airline, route, departure_datetime);

CREATE INDEX idx_staging_airline ON flight_staging(airline);
CREATE INDEX idx_staging_validated ON flight_staging(is_validated);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO airflow;
