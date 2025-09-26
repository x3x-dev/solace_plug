import typing as t
from datetime import datetime, timezone
from pydantic import BaseModel
from solace.messaging.publisher.persistent_message_publisher import PublishReceipt


class PublishReceiptModel(BaseModel):
    """
    Pydantic wrapper around Solace PublishReceipt for structured use in this library.
    """

    message_id: str | None = None
    persisted: bool
    timestamp: datetime
    exception: str | None = None
    user_context: t.Any = None

    @classmethod
    def from_solace(cls, receipt: PublishReceipt) -> "PublishReceiptModel":
        return cls(
            message_id=getattr(receipt.message, "application_message_id", None),
            persisted=receipt.is_persisted,
            timestamp=datetime.fromtimestamp(receipt.time_stamp / 1000, tz=timezone.utc),
            exception=str(receipt.exception) if receipt.exception else None,
            user_context=receipt.user_context,
        )
