
import typing as t
import uuid
from datetime import datetime, timezone
from pydantic import BaseModel, Field


class BaseEvent(BaseModel):
    """
    BaseEvent represents the minimal, shared structure for all events published
    or consumed through Solace in this system.

    Provides:
      - `id`: unique identifier for traceability
      - `timestamp`: UTC creation time
      - `source`: field to identify which service produced the event
      - `payload`: arbitrary event data

    Developers are expected to **subclass BaseEvent** to create their own
    domain-specific events, adding any extra fields needed for their service.

    Example:
        from solace_plug.schemas.base import BaseEvent

        class OrderCreatedEvent(BaseEvent):
            order_id: str
            user_id: str
            amount: float

        # Creating an event
        event = OrderCreatedEvent(
            source="order-service",
            payload={"status": "NEW"},
            order_id="ORD-123",
            user_id="USR-456",
            amount=99.99
        )

        # Serialize for publishing
        json_data = event.to_json()

        # Deserialize on subscriber side
        received = OrderCreatedEvent.from_json(json_data)
        print(received.order_id)  # ORD-123
    """
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str 
    payload: dict

    def to_json(self) -> str:
        return self.model_dump_json()

    def to_dict(self) -> dict:
        return self.model_dump()

    @classmethod
    def from_json(cls, data: str) -> "BaseEvent":
        return cls.model_validate_json(data)