CREATE TABLE IF NOT EXISTS vessels (
    mmsi BIGINT PRIMARY KEY,
    vessel_name VARCHAR(255),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_vessels_last_seen_at ON vessels(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS ix_vessels_name ON vessels(vessel_name);

CREATE TABLE IF NOT EXISTS vessel_positions (
    id BIGSERIAL PRIMARY KEY,
    mmsi BIGINT NOT NULL REFERENCES vessels(mmsi) ON DELETE CASCADE,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    speed_over_ground DOUBLE PRECISION,
    course_over_ground DOUBLE PRECISION,
    event_time TIMESTAMPTZ NOT NULL,
    source VARCHAR(64) NOT NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_vessel_positions_mmsi_event_time ON vessel_positions(mmsi, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_vessel_positions_event_time ON vessel_positions(event_time DESC);
CREATE INDEX IF NOT EXISTS ix_vessel_positions_created_at ON vessel_positions(created_at DESC);

CREATE TABLE IF NOT EXISTS port_weather (
    id BIGSERIAL PRIMARY KEY,
    port_code VARCHAR(32) NOT NULL,
    port_name VARCHAR(255) NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    temperature_c DOUBLE PRECISION,
    wind_speed_kph DOUBLE PRECISION,
    weather_code INTEGER,
    event_time TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_port_weather_port_event_time ON port_weather(port_code, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_port_weather_event_time ON port_weather(event_time DESC);
CREATE INDEX IF NOT EXISTS ix_port_weather_created_at ON port_weather(created_at DESC);

CREATE TABLE IF NOT EXISTS supply_chain_news (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(512) NOT NULL UNIQUE,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    domain VARCHAR(255),
    keywords JSONB NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_supply_chain_news_event_time ON supply_chain_news(event_time DESC);
CREATE INDEX IF NOT EXISTS ix_supply_chain_news_domain_event_time ON supply_chain_news(domain, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_supply_chain_news_created_at ON supply_chain_news(created_at DESC);

CREATE TABLE IF NOT EXISTS risk_events (
    id BIGSERIAL PRIMARY KEY,
    risk_type VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    entity_type VARCHAR(32) NOT NULL,
    entity_id VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    details JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_risk_events_event_time ON risk_events(event_time DESC);
CREATE INDEX IF NOT EXISTS ix_risk_events_severity_event_time ON risk_events(severity, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_risk_events_created_at ON risk_events(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_risk_events_entity ON risk_events(entity_type, entity_id);

CREATE TABLE IF NOT EXISTS consumer_offsets_audit (
    id BIGSERIAL PRIMARY KEY,
    consumer_group VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition INTEGER NOT NULL,
    "offset" BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_consumer_offsets_group_topic ON consumer_offsets_audit(consumer_group, topic);
CREATE INDEX IF NOT EXISTS ix_consumer_offsets_created_at ON consumer_offsets_audit(created_at DESC);
