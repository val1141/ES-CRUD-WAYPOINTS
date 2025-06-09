import uuid
import logging
from typing import List, Optional, Tuple
import clickhouse_connect
import json # <--- ADD THIS IMPORT

from app.config import settings
from app.models import Event, SchedulePoint # Ensure SchedulePoint is imported if used for typing or instantiation

logger = logging.getLogger(__name__)

def get_clickhouse_client():
    try:
        client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DATABASE,
            connect_timeout=15,
            send_receive_timeout=30,
        )
        client.command("SELECT 1")
        logger.debug(f"Successfully connected to ClickHouse: {settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise

def get_current_point_version_and_command(client: clickhouse_connect.driver.client.Client, route_id: uuid.UUID, point_id: uuid.UUID) -> Tuple[int, Optional[uuid.UUID]]:
    query = f"""
    SELECT version
    FROM {settings.CLICKHOUSE_DATABASE}.schedule_points
    WHERE route_id = %(route_id)s AND id = %(point_id)s
    ORDER BY version DESC
    LIMIT 1
    """
    params = {'route_id': str(route_id), 'point_id': str(point_id)}
    logger.debug(f"Querying current version: {query} with params: {params}")
    result = client.query(query, parameters=params)

    if result.result_rows:
        current_version = result.first_row[0]
        logger.debug(f"Current version for point {point_id} on route {route_id} is {current_version}")
        return current_version, None
    logger.debug(f"No existing version for point {point_id} on route {route_id}. Starting at version 0 (next will be 1).")
    return 0, None

def check_command_id_globally_processed(client: clickhouse_connect.driver.client.Client, command_id: uuid.UUID) -> bool:
    query = f"""
    SELECT count()
    FROM {settings.CLICKHOUSE_DATABASE}.schedule_events
    WHERE command_id = %(command_id)s
    """
    params = {'command_id': str(command_id)}
    logger.debug(f"Checking global command_id: {query} with params: {params}")
    result = client.query(query, parameters=params)
    return result.first_row[0] > 0 if result.result_rows and result.first_row else False


def store_event_in_db(client: clickhouse_connect.driver.client.Client, event: Event):
    try:
        event_dict = event.to_db_dict()
        logger.debug(f"Storing event dictionary: {event_dict}")

        cols = ["route_id", "version", "event_id", "command_id", "event_type", "payload", "created_at"]
        event_values_tuple = tuple(event_dict[col_name] for col_name in cols)
        data_to_insert = [event_values_tuple]

        logger.debug(f"Data to insert (list of tuples): {data_to_insert}")
        logger.debug(f"Column names for insert: {cols}")

        client.insert(
            table='schedule_events',
            data=data_to_insert,
            column_names=cols,
            database=settings.CLICKHOUSE_DATABASE
        )
        # Safely extract point_id from payload for logging
        try:
            payload_dict = json.loads(event.payload)
            point_id_from_payload = payload_dict.get('id', 'N/A')
        except json.JSONDecodeError:
            point_id_from_payload = 'ErrorDecodingPayload'
            logger.warning(f"Could not decode JSON payload for event {event.event_id} during logging.")

        logger.info(f"Event {event.event_id} (type: {event.event_type}) stored for route {event.route_id}, point_payload_id {point_id_from_payload}.")
    except Exception as e:
        logger.error(f"Error storing event {event.event_id}: {e}", exc_info=True)
        raise

def get_active_points_for_route(client: clickhouse_connect.driver.client.Client, route_id: uuid.UUID) -> List[SchedulePoint]:
    query = f"""
    SELECT
        id,
        route_id,
        node_id,
        time,
        train_number,
        is_additional_trip,
        trip_type,
        override_color,
        route_changed_at,
        is_deleted
    FROM {settings.CLICKHOUSE_DATABASE}.schedule_points
    WHERE route_id = %(route_id)s AND is_deleted = 0
    ORDER BY time, id
    """
    params = {'route_id': str(route_id)}
    logger.debug(f"Querying active points for route: {query} with params: {params}")
    result = client.query(query, parameters=params)

    points = []
    if result.result_rows:
        # For clickhouse-connect, result.column_names gives the names
        # and result.result_rows gives list of tuples (rows)
        column_names = result.column_names
        for row_values in result.result_rows:
            row_dict = dict(zip(column_names, row_values))
            # Ensure boolean fields are correctly typed from UInt8 if necessary
            # (Pydantic usually handles this if types are correct, but good to be explicit if issues arise)
            if 'is_additional_trip' in row_dict:
                 row_dict['is_additional_trip'] = bool(row_dict['is_additional_trip'])
            if 'is_deleted' in row_dict:
                 row_dict['is_deleted'] = bool(row_dict['is_deleted'])
            points.append(SchedulePoint(**row_dict)) # Assumes SchedulePoint model matches columns
    logger.info(f"Retrieved {len(points)} active points for route_id {route_id}")
    return points

def get_point_events_up_to_version(
    client: clickhouse_connect.driver.client.Client,
    route_id: uuid.UUID,
    point_id: uuid.UUID, # The ID of the point itself, from payload
    target_version: int
) -> List[Event]:
    # We need to filter by point_id within the JSON payload. This can be less efficient.
    # If this query becomes slow, consider adding point_id as a top-level column in schedule_events,
    # or creating a materialized view specifically for point history.
    query = f"""
    SELECT route_id, version, event_id, command_id, event_type, payload, created_at
    FROM {settings.CLICKHOUSE_DATABASE}.schedule_events
    WHERE route_id = %(route_id)s
      AND toUUID(JSONExtractString(payload, 'id')) = %(point_id)s
      AND version <= %(target_version)s
      AND event_type = 'SchedulePointUpserted' -- Assuming only this type modifies point state
    ORDER BY version ASC
    """
    params = {
        'route_id': str(route_id),
        'point_id': str(point_id),
        'target_version': target_version
    }
    logger.debug(f"Querying events for point {point_id} on route {route_id} up to version {target_version}: {query} with params {params}")
    result = client.query(query, parameters=params)

    events = []
    if result.result_rows:
        column_names = result.column_names
        for row_values in result.result_rows:
            event_dict = dict(zip(column_names, row_values))
            # Deserialize payload back into a SchedulePoint or keep as string for Event model
            # The Event model expects payload as a string, so this is fine.
            events.append(Event(**event_dict))
    logger.info(f"Retrieved {len(events)} events for point {point_id} on route {route_id} up to version {target_version}.")
    return events

def reconstruct_point_state_from_events(events: List[Event]) -> Optional[SchedulePoint]:
    if not events:
        return None

    # Sort events by version just in case they aren't already (though query does this)
    # events.sort(key=lambda e: e.version) # Already ordered by query

    current_state_dict = {}
    for event in events:
        if event.event_type == "SchedulePointUpserted":
            try:
                payload_data = json.loads(event.payload)
                # Simply override with the new payload. In a more complex system with
                # different event types (e.g., PointTimeChanged, PointColorChanged),
                # you'd apply changes more granularly.
                current_state_dict = payload_data
            except json.JSONDecodeError:
                logger.error(f"Failed to decode payload for event {event.event_id} during reconstruction.")
                continue # or raise error
        # Add other event_type handling here if applicable

    if not current_state_dict:
        return None

    # The reconstructed state should have the version of the last event applied.
    # However, SchedulePoint model itself doesn't have 'version'.
    # We are returning the state *as of* that version.
    return SchedulePoint(**current_state_dict)

def get_point_event_history(
    client: clickhouse_connect.driver.client.Client,
    route_id: uuid.UUID,
    point_id: uuid.UUID # The ID of the point itself, from payload
) -> List[Event]:
    query = f"""
    SELECT route_id, version, event_id, command_id, event_type, payload, created_at
    FROM {settings.CLICKHOUSE_DATABASE}.schedule_events
    WHERE route_id = %(route_id)s
      AND toUUID(JSONExtractString(payload, 'id')) = %(point_id)s
    ORDER BY version ASC
    """
    params = {
        'route_id': str(route_id),
        'point_id': str(point_id)
    }
    logger.debug(f"Querying event history for point {point_id} on route {route_id}: {query} with params {params}")
    result = client.query(query, parameters=params)

    events = []
    if result.result_rows:
        column_names = result.column_names
        for row_values in result.result_rows:
            event_dict = dict(zip(column_names, row_values))
            events.append(Event(**event_dict))
    logger.info(f"Retrieved {len(events)} events for history of point {point_id} on route {route_id}.")
    return events