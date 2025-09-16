import logging
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener
from solace.messaging.messaging_service import ServiceInterruptionListener, RetryStrategy, ServiceEvent


log = logging.getLogger("solace-plug")


class SolaceClient:
    """Reusable Solace connection client for pub/sub operations"""