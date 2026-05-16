\connect nasa_neo
Create table if not exists ignored(
    asteroid_id BIGINT,
    name VARCHAR(30),
    absolute_magnitude FLOAT,
    diameter_min_m FLOAT,
    diameter_max_m FLOAT,
    velocity_km_s FLOAT,
    miss_distance_km FLOAT,
    date date,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asteroid_id, date));