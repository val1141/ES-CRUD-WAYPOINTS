from datetime import datetime
from uuid import uuid4
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.models import Event, SchedulePoint
from app.db import reconstruct_point_state_from_events


def make_event(route_id, point_id, version, time_value):
    point = SchedulePoint(
        id=point_id,
        route_id=route_id,
        node_id=uuid4(),
        time=time_value,
        train_number=1,
        is_additional_trip=False,
        trip_type=1,
        override_color=None,
        route_changed_at=datetime.utcnow(),
        is_deleted=False,
    )
    return Event(
        route_id=route_id,
        version=version,
        command_id=uuid4(),
        event_type="SchedulePointUpserted",
        payload=point.model_dump_json(),
    )


def test_reconstruct_state_uses_last_event():
    route_id = uuid4()
    point_id = uuid4()
    events = [
        make_event(route_id, point_id, 1, 10),
        make_event(route_id, point_id, 2, 20),
    ]

    state = reconstruct_point_state_from_events(events)

    assert state is not None
    assert state.id == point_id
    assert state.time == 20
