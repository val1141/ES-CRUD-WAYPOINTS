-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS schedule_service_db;

USE schedule_service_db;

-- Drop dependent MV first, then table
DROP VIEW IF EXISTS schedule_points_mv;
DROP TABLE IF EXISTS schedule_points;
DROP TABLE IF EXISTS schedule_events; -- If you want a completely fresh start for events too for testing

-- Event Store (append-only)
CREATE TABLE IF NOT EXISTS schedule_events (
  route_id UUID,
  version UInt64,
  event_id UUID,
  command_id UUID,
  event_type String,
  payload String,
  created_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (route_id, version, command_id);

-- Read-model Target Table
CREATE TABLE IF NOT EXISTS schedule_points (
    id UUID,
    route_id UUID,
    node_id UUID,
    time Int32,
    train_number Int32,
    is_additional_trip UInt8,
    trip_type Int32,
    override_color Nullable(String),
    route_changed_at Nullable(DateTime64(3)),
    version UInt64,
    is_deleted UInt8
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (route_id, id);

-- Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS schedule_points_mv
TO schedule_points
AS
SELECT
  anyLast(extracted.point_id)                 AS id,
  extracted.route_id_from_event_table         AS route_id,
  anyLast(extracted.node_id)                  AS node_id,
  anyLast(extracted.time)                     AS time,
  anyLast(extracted.train_number)             AS train_number,
  anyLast(extracted.is_additional_trip)       AS is_additional_trip,
  anyLast(extracted.trip_type)                AS trip_type,
  anyLast(extracted.override_color)           AS override_color,
  anyLast(extracted.route_changed_at)         AS route_changed_at,
  max(extracted.event_version)                AS version,
  anyLast(extracted.is_deleted)               AS is_deleted
FROM (
  SELECT
    route_id AS route_id_from_event_table,
    version AS event_version,
    toUUID(JSONExtractString(payload, 'id'))                 AS point_id,
    toUUID(JSONExtractString(payload, 'node_id'))            AS node_id,
    JSONExtractInt(payload, 'time')                          AS time,
    JSONExtractInt(payload, 'train_number')                  AS train_number,
    JSONExtractBool(payload, 'is_additional_trip')           AS is_additional_trip,
    JSONExtractInt(payload, 'trip_type')                     AS trip_type,
    JSONExtractString(payload, 'override_color')             AS override_color,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'route_changed_at')) AS route_changed_at,
    JSONExtractBool(payload, 'is_deleted')                   AS is_deleted
  FROM schedule_events
) AS extracted
GROUP BY extracted.route_id_from_event_table, extracted.point_id;