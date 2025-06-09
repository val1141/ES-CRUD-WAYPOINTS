import uuid
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

# --- Business Entity ---
class SchedulePoint(BaseModel):
    id: uuid.UUID # Point's own ID
    route_id: uuid.UUID # Aggregate ID
    node_id: uuid.UUID
    time: int
    train_number: int
    is_additional_trip: bool
    trip_type: int
    override_color: Optional[str] = None
    route_changed_at: datetime # Expected as ISO string "YYYY-MM-DDTHH:MM:SSZ" or similar
    is_deleted: bool = False # Default to false

# --- API Payloads ---
class PointUpsertRequest(BaseModel):
    point: SchedulePoint
    command_id: uuid.UUID = Field(default_factory=uuid.uuid4)

# --- Event Sourcing Models ---
# EventPayload will be the SchedulePoint itself
class Event(BaseModel):
    route_id: uuid.UUID
    version: int
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    command_id: uuid.UUID
    event_type: str # e.g., "SchedulePointUpserted"
    payload: str # JSON string of SchedulePoint
    created_at: datetime = Field(default_factory=datetime.utcnow)

    def to_db_dict(self):
        return {
            "route_id": self.route_id,
            "version": self.version,
            "event_id": self.event_id,
            "command_id": self.command_id,
            "event_type": self.event_type,
            "payload": self.payload,
            "created_at": self.created_at,
        }

# --- API Response Models ---
class SchedulePointResponse(SchedulePoint):
    # Inherits all fields from SchedulePoint
    # In this case, it's identical to SchedulePoint,
    # but defined for clarity and future extensibility if response differs.
    pass

class RevertCommand(BaseModel):
    command_id: uuid.UUID = Field(default_factory=uuid.uuid4)