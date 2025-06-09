import uuid
import json
import logging
from fastapi import FastAPI, HTTPException, Depends, status, Path as FastAPIPath
from typing import List, Optional

from app.models import PointUpsertRequest, Event, SchedulePoint, SchedulePointResponse, RevertCommand
from app.db import (
    get_clickhouse_client,
    store_event_in_db,
    get_current_point_version_and_command,
    check_command_id_globally_processed,
    get_active_points_for_route,
    get_point_events_up_to_version,
    reconstruct_point_state_from_events,
    get_point_event_history
)
from app.config import settings
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="SchedulePoint Service with Event Sourcing")

# Dependency for ClickHouse client
def get_db():
    client = None
    try:
        client = get_clickhouse_client()
        yield client
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable (DB connection error)")
    finally:
        if client:
            client.close()

@app.on_event("startup")
async def startup_event():
    logger.info("Application startup...")
    # Attempt to connect to ClickHouse to ensure it's available and tables exist
    # This is a good place to run migrations or checks in a real app
    try:
        client = get_clickhouse_client()
        # Basic check if tables exist (optional, init script should handle creation)
        client.command(f"SELECT 1 FROM {settings.CLICKHOUSE_DATABASE}.schedule_events LIMIT 1")
        client.command(f"SELECT 1 FROM {settings.CLICKHOUSE_DATABASE}.schedule_points LIMIT 1")
        logger.info("Successfully connected to ClickHouse on startup and tables seem to exist.")
        client.close()
    except Exception as e:
        logger.error(f"Failed to verify ClickHouse setup on startup: {e}. "
                     "Ensure ClickHouse is running and init.sql has been executed.")
        # Depending on severity, you might want to prevent startup
        # raise RuntimeError("Could not verify ClickHouse setup on startup")


@app.post("/points", status_code=status.HTTP_202_ACCEPTED, response_model=Event)
async def upsert_point(
    request_data: PointUpsertRequest, # PointUpsertRequest теперь должен содержать expected_version
    client: clickhouse_connect.driver.client.Client = Depends(get_db)
):
    """
    Создает или обновляет SchedulePoint путем сохранения события.
    Использует command_id для идемпотентности.
    Использует expected_version для optimistic locking.
    """
    point_data = request_data.point
    command_id = request_data.command_id
    expected_version = request_data.expected_version # Получаем ожидаемую версию из запроса
    point_id = point_data.id
    route_id = point_data.route_id

    logger.info(
        f"Received upsert command {command_id} for point {point_id} on route {route_id}. "
        f"Expected version: {expected_version}. Payload: {point_data.model_dump(mode='json', exclude_none=True)}"
    )

    # 1. Строгая проверка идемпотентности по command_id
    if check_command_id_globally_processed(client, command_id):
        logger.warning(f"Command ID {command_id} has already been processed. Returning 409 Conflict.")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Command ID {command_id} has already been processed. "
                   "If this was an intentional retry for a previously failed request, "
                   "please use a new command_id for new data or ensure the original request completed."
        )

    try:
        # 2. Optimistic Locking: Получаем текущую сохраненную версию точки из MV
        # Эта версия используется для проверки expected_version и для вычисления следующей версии события.
        # current_stored_version будет 0, если точка не найдена в MV (т.е. это новая точка).
        current_stored_version, _ = get_current_point_version_and_command(client, route_id, point_id)

        logger.debug(f"Point {point_id} on route {route_id}: current actual version from MV is {current_stored_version}.")

        # 3. Проверяем optimistic lock
        # Клиент должен передать 0 в expected_version, если он создает новую точку.
        # Клиент должен передать N в expected_version, если он обновляет точку, которая, как он ожидает, имеет версию N.
        if expected_version != current_stored_version:
            error_message = (
                f"Optimistic lock failed for point {point_id} on route {route_id}. "
                f"Client expected version {expected_version}, but current stored version is {current_stored_version}."
            )
            logger.error(error_message)
            raise HTTPException(
                status_code=status.HTTP_412_PRECONDITION_FAILED, # 412 Precondition Failed
                detail=error_message + " Please refresh point data and try again."
            )

        # 4. Если optimistic lock прошел, следующая версия события будет на 1 больше текущей сохраненной версии.
        next_event_version = current_stored_version + 1

        logger.info(
            f"Optimistic lock passed for point {point_id}. "
            f"Proceeding to create event with version {next_event_version} (based on current stored version {current_stored_version})."
        )

        # 5. Создаем событие
        event = Event(
            route_id=route_id,
            version=next_event_version, # Используем новую рассчитанную версию
            command_id=command_id,
            event_type="SchedulePointUpserted", # Единый тип события для создания/обновления
            payload=point_data.model_dump_json(), # Сериализуем Pydantic модель в JSON строку
            # event_id и created_at генерируются автоматически моделью Event
        )

        # 6. Сохраняем событие в БД
        store_event_in_db(client, event)
        logger.info(
            f"Event {event.event_id} (type: {event.event_type}) for command {command_id} "
            f"(point {point_id}, new version {next_event_version}) successfully stored."
        )

        # HTTP 202 Accepted: Запрос принят к обработке, но обработка не обязательно завершена
        # (обновление MV происходит асинхронно). Клиент получит данные о созданном событии.
        return event

    except HTTPException:
        # Перебрасываем HTTPException, чтобы сохранить статус код и детали
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing command {command_id} for point {point_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@app.get("/routes/{route_id}/points", response_model=List[SchedulePointResponse])
async def get_route_points(
    route_id: uuid.UUID = FastAPIPath(..., description="The ID of the route to retrieve points for"),
    client: clickhouse_connect.driver.client.Client = Depends(get_db)
):
    """
    Retrieves all actual (non-deleted) schedule points for a given route_id
    from the materialized read model.
    """
    logger.info(f"Request to get active points for route_id: {route_id}")
    try:
        points = get_active_points_for_route(client, route_id)
        # No special handling for "not found" needed if an empty list is acceptable.
        # If route_id itself must exist, additional checks might be needed.
        return points
    except Exception as e:
        logger.error(f"Error retrieving points for route_id {route_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@app.get("/routes/{route_id}/points/{point_id}/versions/{version}", response_model=Optional[SchedulePointResponse])
async def get_point_at_version(
    route_id: uuid.UUID = FastAPIPath(..., description="Route ID"),
    point_id: uuid.UUID = FastAPIPath(..., description="Schedule Point ID"),
    version: int = FastAPIPath(..., description="Target version number", ge=1),
    client: clickhouse_connect.driver.client.Client = Depends(get_db)
):
    """
    Retrieves the state of a specific SchedulePoint for a given route
    as it was at the specified version.
    """
    logger.info(f"Request to get point {point_id} on route {route_id} at version {version}")
    try:
        events = get_point_events_up_to_version(client, route_id, point_id, version)
        if not events:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"No events found for point {point_id} on route {route_id} up to version {version}, or point does not exist.")

        reconstructed_state = reconstruct_point_state_from_events(events)
        if not reconstructed_state:
            # This case should ideally be caught by 'if not events' unless reconstruction fails weirdly
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Could not reconstruct state for point {point_id} on route {route_id} at version {version}.")

        return reconstructed_state # Pydantic will serialize SchedulePoint to SchedulePointResponse
    except HTTPException:
        raise # Re-raise HTTPException to preserve status code and detail
    except Exception as e:
        logger.error(f"Error getting point {point_id} at version {version}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
@app.get("/routes/{route_id}/points/{point_id}/history", response_model=List[Event])
async def get_point_history(
    route_id: uuid.UUID = FastAPIPath(..., description="Route ID"),
    point_id: uuid.UUID = FastAPIPath(..., description="Schedule Point ID"),
    client: clickhouse_connect.driver.client.Client = Depends(get_db)
):
    """
    Retrieves the complete event history for a specific SchedulePoint on a given route.
    """
    logger.info(f"Request for event history of point {point_id} on route {route_id}")
    try:
        events = get_point_event_history(client, route_id, point_id)
        # If no events, an empty list is a valid response
        return events
    except Exception as e:
        logger.error(f"Error getting history for point {point_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
@app.post("/routes/{route_id}/points/{point_id}/revert-to-version/{target_version}",
           status_code=status.HTTP_202_ACCEPTED,
           response_model=Event)
async def revert_point_to_version(
    revert_command: RevertCommand, # Body parameter moved first (among those that can be reordered)
    route_id: uuid.UUID = FastAPIPath(..., description="Route ID"),
    point_id: uuid.UUID = FastAPIPath(..., description="Schedule Point ID"),
    target_version: int = FastAPIPath(..., description="Version to revert to", ge=1),
    client: clickhouse_connect.driver.client.Client = Depends(get_db) # Default argument last
):
    # ... rest of the function body remains the same
    command_id = revert_command.command_id
    logger.info(f"Received revert command {command_id} for point {point_id} on route {route_id} to version {target_version}")

    # Idempotency Check (optional, similar to upsert)
    if check_command_id_globally_processed(client, command_id):
        logger.warning(f"Revert Command ID {command_id} has been processed before. Proceeding.")

    try:
        current_mv_version, _ = get_current_point_version_and_command(client, route_id, point_id)
        if current_mv_version == 0:
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Point {point_id} on route {route_id} not found.")
        if target_version >= current_mv_version: # target_version must be strictly less
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Target version {target_version} must be less than current version {current_mv_version}. Point not found or target is not historical.")

        historical_events = get_point_events_up_to_version(client, route_id, point_id, target_version)
        if not historical_events: # Should not happen if target_version < current_mv_version and point exists
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"No history found for point {point_id} up to version {target_version}.")

        state_to_revert_to = reconstruct_point_state_from_events(historical_events)
        if not state_to_revert_to:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Could not reconstruct state for point {point_id} at version {target_version}.")

        if state_to_revert_to.id != point_id or state_to_revert_to.route_id != route_id:
             logger.error(f"Mismatch in reconstructed state IDs: payload has {state_to_revert_to.id}/{state_to_revert_to.route_id}, expected {point_id}/{route_id}")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="State reconstruction ID mismatch.")

        next_event_version = current_mv_version + 1
        
        revert_event = Event(
            route_id=route_id,
            version=next_event_version,
            command_id=command_id,
            event_type="SchedulePointUpserted",
            payload=state_to_revert_to.model_dump_json(),
        )

        store_event_in_db(client, revert_event)
        logger.info(f"Revert Event {revert_event.event_id} (new v{next_event_version}) stored for point {point_id}, reverting to state of v{target_version}.")

        return revert_event

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing revert command {command_id} for point {point_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))