-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS schedule_service_db;

-- Switch to the database
USE schedule_service_db;

-- Event Store (append-only)
CREATE TABLE IF NOT EXISTS schedule_events (
  route_id UUID,
  version UInt64,        -- Version of the specific point (route_id, point_id from payload)
  event_id UUID,
  command_id UUID,
  event_type String,
  payload String,        -- JSON string of SchedulePoint
  created_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (route_id, version, command_id);

-- Read-model Target Table (for Materialized View)
CREATE TABLE IF NOT EXISTS schedule_points (
    id UUID,                 -- Point's own ID
    route_id UUID,
    node_id UUID,
    time Int32,
    train_number Int32,
    is_additional_trip UInt8, -- Boolean stored as 0 or 1
    trip_type Int32,
    override_color Nullable(String),
    route_changed_at Nullable(DateTime64(3)),
    version UInt64,          -- Latest version of this point
    is_deleted UInt8         -- Boolean stored as 0 or 1
)
ENGINE = ReplacingMergeTree(version) -- Keeps only the row with the highest version for the sort key
ORDER BY (route_id, id);             -- Sort key for ReplacingMergeTree (aggregate_id, entity_id)

-- Materialized View to project events into the current state of schedule points
CREATE MATERIALIZED VIEW IF NOT EXISTS schedule_points_mv
TO schedule_points
AS
SELECT
  toUUID(JSONExtractString(payload, 'id'))                                      AS id,
  route_id,
  anyLast(toUUID(JSONExtractString(payload, 'node_id')))                        AS node_id,
  anyLast(JSONExtractInt(payload, 'time'))                                      AS time,
  anyLast(JSONExtractInt(payload, 'train_number'))                              AS train_number,
  anyLast(JSONExtractBool(payload, 'is_additional_trip'))                       AS is_additional_trip,
  anyLast(JSONExtractInt(payload, 'trip_type'))                                 AS trip_type,
  anyLast(JSONExtractString(payload, 'override_color'))                         AS override_color,
  anyLast(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'route_changed_at'))) AS route_changed_at,
  max(version)                                                                  AS version,
  max(JSONExtractBool(payload, 'is_deleted'))                                   AS is_deleted
FROM schedule_events
GROUP BY route_id, id;