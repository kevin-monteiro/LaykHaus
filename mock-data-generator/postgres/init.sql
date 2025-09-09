-- Solar Energy Demo Database Schema
-- This creates realistic tables for a solar energy monitoring system

-- Clean up if exists
DROP SCHEMA IF EXISTS solar CASCADE;
CREATE SCHEMA solar;

-- Set default schema
SET search_path TO solar;

-- 1. Solar Panels Table (Static/Reference Data)
CREATE TABLE solar_panels (
    panel_id SERIAL PRIMARY KEY,
    serial_number VARCHAR(50) UNIQUE NOT NULL,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    capacity_watts INTEGER,
    efficiency_percentage DECIMAL(5,2),
    installation_date DATE,
    location_latitude DECIMAL(10,7),
    location_longitude DECIMAL(10,7),
    location_name VARCHAR(200),
    status VARCHAR(20) DEFAULT 'active',
    last_maintenance_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Weather Stations Table
CREATE TABLE weather_stations (
    station_id SERIAL PRIMARY KEY,
    station_code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(100),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    elevation_meters INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Energy Production Table (Historical Data)
CREATE TABLE energy_production (
    id SERIAL PRIMARY KEY,
    panel_id INTEGER REFERENCES solar_panels(panel_id),
    timestamp TIMESTAMP NOT NULL,
    power_output_watts DECIMAL(10,2),
    voltage DECIMAL(8,2),
    current_amps DECIMAL(8,2),
    temperature_celsius DECIMAL(5,2),
    efficiency_actual DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_panel_timestamp ON energy_production (panel_id, timestamp);

-- 4. Weather Data Table (Historical)
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES weather_stations(station_id),
    timestamp TIMESTAMP NOT NULL,
    temperature_celsius DECIMAL(5,2),
    humidity_percentage DECIMAL(5,2),
    wind_speed_ms DECIMAL(6,2),
    wind_direction_degrees INTEGER,
    solar_radiation_wm2 DECIMAL(8,2),
    cloud_cover_percentage INTEGER,
    precipitation_mm DECIMAL(6,2),
    uv_index DECIMAL(4,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_station_timestamp ON weather_data (station_id, timestamp);

-- 5. Customers Table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    email VARCHAR(200) UNIQUE,
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    customer_type VARCHAR(20), -- 'residential', 'commercial', 'industrial'
    contract_start_date DATE,
    monthly_subscription DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Customer Panel Mapping
CREATE TABLE customer_panels (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    panel_id INTEGER REFERENCES solar_panels(panel_id),
    ownership_percentage DECIMAL(5,2) DEFAULT 100.00,
    start_date DATE NOT NULL,
    end_date DATE,
    UNIQUE(customer_id, panel_id, start_date)
);

-- 7. Energy Consumption Table
CREATE TABLE energy_consumption (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    timestamp TIMESTAMP NOT NULL,
    consumption_kwh DECIMAL(10,3),
    peak_demand_kw DECIMAL(8,3),
    off_peak_kwh DECIMAL(10,3),
    cost_dollars DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_timestamp ON energy_consumption (customer_id, timestamp);

-- 8. Maintenance Logs
CREATE TABLE maintenance_logs (
    log_id SERIAL PRIMARY KEY,
    panel_id INTEGER REFERENCES solar_panels(panel_id),
    maintenance_date TIMESTAMP NOT NULL,
    maintenance_type VARCHAR(50), -- 'routine', 'repair', 'cleaning', 'replacement'
    description TEXT,
    cost DECIMAL(10,2),
    technician_name VARCHAR(100),
    next_maintenance_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 9. Alerts Table
CREATE TABLE alerts (
    alert_id SERIAL PRIMARY KEY,
    panel_id INTEGER REFERENCES solar_panels(panel_id),
    alert_type VARCHAR(50), -- 'low_efficiency', 'offline', 'maintenance_due', 'weather_warning'
    severity VARCHAR(20), -- 'info', 'warning', 'critical'
    message TEXT,
    triggered_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 10. Daily Aggregates (for faster queries)
CREATE TABLE daily_production_summary (
    id SERIAL PRIMARY KEY,
    panel_id INTEGER REFERENCES solar_panels(panel_id),
    date DATE NOT NULL,
    total_energy_kwh DECIMAL(10,3),
    peak_power_watts DECIMAL(10,2),
    average_efficiency DECIMAL(5,2),
    operating_hours DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(panel_id, date)
);

-- Insert Initial Demo Data

-- Weather Stations
INSERT INTO weather_stations (station_code, name, latitude, longitude, elevation_meters) VALUES
('WS001', 'Phoenix Solar Farm Station', 33.4484, -112.0740, 331),
('WS002', 'California Desert Station', 34.8697, -116.8345, 585),
('WS003', 'Texas Solar Ranch Station', 30.2672, -97.7431, 149),
('WS004', 'Nevada Solar Array Station', 36.1699, -115.1398, 620),
('WS005', 'Florida Sunshine Station', 27.9944, -82.5366, 15);

-- Solar Panels (100 panels across different locations)
INSERT INTO solar_panels (serial_number, manufacturer, model, capacity_watts, efficiency_percentage, installation_date, location_latitude, location_longitude, location_name, status)
SELECT 
    'SP-2024-' || LPAD(generate_series::text, 5, '0'),
    CASE (random() * 3)::int 
        WHEN 0 THEN 'SunPower'
        WHEN 1 THEN 'Tesla Solar'
        WHEN 2 THEN 'Canadian Solar'
        ELSE 'LG Solar'
    END,
    CASE (random() * 3)::int
        WHEN 0 THEN 'X-Series X22-370'
        WHEN 1 THEN 'PowerXT-400'
        WHEN 2 THEN 'HiDM CS1H-335'
        ELSE 'NeON 2 Black'
    END,
    350 + (random() * 150)::int,
    18 + (random() * 5)::numeric(5,2),
    CURRENT_DATE - (random() * 1000)::int,
    33.4484 + (random() * 5 - 2.5)::numeric(10,7),
    -112.0740 + (random() * 5 - 2.5)::numeric(10,7),
    CASE ((generate_series - 1) / 20)
        WHEN 0 THEN 'Phoenix Solar Farm'
        WHEN 1 THEN 'California Desert Array'
        WHEN 2 THEN 'Texas Solar Ranch'
        WHEN 3 THEN 'Nevada Solar Park'
        ELSE 'Florida Sunshine Field'
    END,
    CASE WHEN random() > 0.95 THEN 'maintenance' ELSE 'active' END
FROM generate_series(1, 100);

-- Customers (50 customers)
INSERT INTO customers (customer_code, name, email, phone, address, city, state, country, customer_type, contract_start_date, monthly_subscription)
SELECT
    'CUST-' || LPAD(generate_series::text, 5, '0'),
    'Customer ' || generate_series,
    'customer' || generate_series || '@example.com',
    '+1-555-' || LPAD((1000 + generate_series)::text, 4, '0'),
    generate_series || ' Solar Street',
    CASE (random() * 4)::int
        WHEN 0 THEN 'Phoenix'
        WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Austin'
        WHEN 3 THEN 'Las Vegas'
        ELSE 'Miami'
    END,
    CASE (random() * 4)::int
        WHEN 0 THEN 'Arizona'
        WHEN 1 THEN 'California'
        WHEN 2 THEN 'Texas'
        WHEN 3 THEN 'Nevada'
        ELSE 'Florida'
    END,
    'USA',
    CASE 
        WHEN generate_series <= 30 THEN 'residential'
        WHEN generate_series <= 45 THEN 'commercial'
        ELSE 'industrial'
    END,
    CURRENT_DATE - (random() * 730)::int,
    100 + (random() * 900)::numeric(10,2)
FROM generate_series(1, 50);

-- Map customers to panels (each customer gets 1-3 panels)
INSERT INTO customer_panels (customer_id, panel_id, ownership_percentage, start_date)
SELECT 
    c.customer_id,
    p.panel_id,
    100.00,
    c.contract_start_date
FROM customers c
CROSS JOIN LATERAL (
    SELECT panel_id 
    FROM solar_panels 
    ORDER BY random() 
    LIMIT 1 + (random() * 2)::int
) p;

-- Generate last 30 days of energy production data
INSERT INTO energy_production (panel_id, timestamp, power_output_watts, voltage, current_amps, temperature_celsius, efficiency_actual)
SELECT 
    p.panel_id,
    ts.timestamp,
    -- Simulate daily production curve (peak at noon)
    CASE 
        WHEN EXTRACT(hour FROM ts.timestamp) BETWEEN 6 AND 18 THEN
            p.capacity_watts * (0.3 + 0.7 * SIN(RADIANS((EXTRACT(hour FROM ts.timestamp) - 6) * 15))) * (0.8 + random() * 0.2)
        ELSE 0
    END,
    CASE 
        WHEN EXTRACT(hour FROM ts.timestamp) BETWEEN 6 AND 18 THEN 
            380 + random() * 40
        ELSE 0
    END,
    CASE 
        WHEN EXTRACT(hour FROM ts.timestamp) BETWEEN 6 AND 18 THEN 
            5 + random() * 10
        ELSE 0
    END,
    20 + random() * 20,
    p.efficiency_percentage * (0.8 + random() * 0.2)
FROM solar_panels p
CROSS JOIN (
    SELECT generate_series(
        CURRENT_TIMESTAMP - INTERVAL '30 days',
        CURRENT_TIMESTAMP,
        INTERVAL '1 hour'
    ) AS timestamp
) ts
WHERE p.status = 'active';

-- Generate weather data for last 30 days
INSERT INTO weather_data (station_id, timestamp, temperature_celsius, humidity_percentage, wind_speed_ms, solar_radiation_wm2, cloud_cover_percentage)
SELECT 
    ws.station_id,
    ts.timestamp,
    20 + random() * 25,  -- Temperature 20-45°C
    20 + random() * 60,  -- Humidity 20-80%
    random() * 15,       -- Wind speed 0-15 m/s
    CASE 
        WHEN EXTRACT(hour FROM ts.timestamp) BETWEEN 6 AND 18 THEN
            800 * SIN(RADIANS((EXTRACT(hour FROM ts.timestamp) - 6) * 15)) * (0.7 + random() * 0.3)
        ELSE 0
    END,
    random() * 100      -- Cloud cover 0-100%
FROM weather_stations ws
CROSS JOIN (
    SELECT generate_series(
        CURRENT_TIMESTAMP - INTERVAL '30 days',
        CURRENT_TIMESTAMP,
        INTERVAL '1 hour'
    ) AS timestamp
) ts;

-- Generate energy consumption data
INSERT INTO energy_consumption (customer_id, timestamp, consumption_kwh, peak_demand_kw, cost_dollars)
SELECT 
    c.customer_id,
    ts.timestamp,
    CASE c.customer_type
        WHEN 'residential' THEN 1 + random() * 5
        WHEN 'commercial' THEN 10 + random() * 50
        ELSE 50 + random() * 200
    END,
    CASE c.customer_type
        WHEN 'residential' THEN 3 + random() * 7
        WHEN 'commercial' THEN 20 + random() * 80
        ELSE 100 + random() * 400
    END,
    CASE c.customer_type
        WHEN 'residential' THEN 0.5 + random() * 2
        WHEN 'commercial' THEN 5 + random() * 20
        ELSE 20 + random() * 100
    END
FROM customers c
CROSS JOIN (
    SELECT generate_series(
        CURRENT_TIMESTAMP - INTERVAL '30 days',
        CURRENT_TIMESTAMP,
        INTERVAL '1 hour'
    ) AS timestamp
) ts;

-- Generate some maintenance logs
INSERT INTO maintenance_logs (panel_id, maintenance_date, maintenance_type, description, cost, technician_name, next_maintenance_date)
SELECT 
    panel_id,
    CURRENT_TIMESTAMP - (random() * 365 || ' days')::interval,
    CASE (random() * 3)::int
        WHEN 0 THEN 'cleaning'
        WHEN 1 THEN 'routine'
        WHEN 2 THEN 'repair'
        ELSE 'inspection'
    END,
    'Regular maintenance performed',
    50 + random() * 450,
    'Tech ' || (1 + random() * 10)::int,
    CURRENT_DATE + (30 + random() * 60)::int
FROM solar_panels
WHERE random() < 0.3;

-- Generate some alerts
INSERT INTO alerts (panel_id, alert_type, severity, message, triggered_at)
SELECT 
    panel_id,
    CASE (random() * 3)::int
        WHEN 0 THEN 'low_efficiency'
        WHEN 1 THEN 'offline'
        WHEN 2 THEN 'maintenance_due'
        ELSE 'weather_warning'
    END,
    CASE (random() * 2)::int
        WHEN 0 THEN 'info'
        WHEN 1 THEN 'warning'
        ELSE 'critical'
    END,
    'Alert triggered for panel',
    CURRENT_TIMESTAMP - (random() * 7 || ' days')::interval
FROM solar_panels
WHERE random() < 0.2;

-- Create daily summaries for faster queries
INSERT INTO daily_production_summary (panel_id, date, total_energy_kwh, peak_power_watts, average_efficiency, operating_hours)
SELECT 
    panel_id,
    DATE(timestamp),
    SUM(power_output_watts) / 1000,
    MAX(power_output_watts),
    AVG(efficiency_actual),
    COUNT(CASE WHEN power_output_watts > 0 THEN 1 END)
FROM energy_production
GROUP BY panel_id, DATE(timestamp);

-- Create useful indexes
CREATE INDEX idx_energy_production_timestamp ON energy_production(timestamp);
CREATE INDEX idx_weather_data_timestamp ON weather_data(timestamp);
CREATE INDEX idx_energy_consumption_timestamp ON energy_consumption(timestamp);
CREATE INDEX idx_alerts_triggered ON alerts(triggered_at);
CREATE INDEX idx_daily_summary_date ON daily_production_summary(date);

-- Create views for common queries
CREATE VIEW current_panel_status AS
SELECT 
    sp.panel_id,
    sp.serial_number,
    sp.location_name,
    sp.status,
    COALESCE(latest.power_output_watts, 0) as current_power,
    COALESCE(latest.efficiency_actual, 0) as current_efficiency,
    latest.temperature_celsius
FROM solar_panels sp
LEFT JOIN LATERAL (
    SELECT * FROM energy_production ep
    WHERE ep.panel_id = sp.panel_id
    ORDER BY timestamp DESC
    LIMIT 1
) latest ON true;

CREATE VIEW customer_dashboard AS
SELECT 
    c.customer_id,
    c.name,
    c.customer_type,
    COUNT(DISTINCT cp.panel_id) as panel_count,
    SUM(p.capacity_watts) as total_capacity_watts,
    AVG(p.efficiency_percentage) as avg_efficiency
FROM customers c
LEFT JOIN customer_panels cp ON c.customer_id = cp.customer_id
LEFT JOIN solar_panels p ON cp.panel_id = p.panel_id
GROUP BY c.customer_id, c.name, c.customer_type;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA solar TO demo_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA solar TO demo_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA solar TO demo_user;