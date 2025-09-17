import logging
import asyncio
from contextlib import contextmanager, asynccontextmanager
import typing as t
from solace.messaging.messaging_service import MessagingService
from .log import log

class SolaceClient:
    """
    Synchronous Solace connection client.
    For simple scripts and synchronous workflows.
    """

    def __init__(
        self,
        host: str = "tcp://localhost:55554",
        vpn: str = "default",
        username: str = "admin",
        password: str = "admin",
        client_name: str | None = None,
    ):
        self.host = host
        self.vpn = vpn
        self.username = username
        self.password = password
        self.client_name = client_name or f"solace-plug-{id(self)}"
        self._service: t.Optional[MessagingService] = None
        self._connected = False

        # Connection properties
        self._broker_props = {
            "solace.messaging.transport.host": host,
            "solace.messaging.service.vpn-name": vpn,
            "solace.messaging.authentication.scheme.basic.username": username,
            "solace.messaging.authentication.scheme.basic.password": password,
            "solace.messaging.client.name": self.client_name
        }

    @property
    def is_connected(self):
        return self._connected

    def connect(self):
        """Connect to Solace"""
        if self._connected:
            return

        log.info("Connecting to Solace at %s (vpn=%s)...", self.host, self.vpn)
        self._service = MessagingService.builder().from_properties(self._broker_props).build()
        self._service.connect()
        self._connected = True
        log.info("Connected to Solace")

    def disconnect(self):
        """Disconnect from Solace"""
        if not self._connected or not self._service:
            return

        log.info("Disconnecting from Solace...")
        self._service.disconnect()
        self._connected = False
        log.info("Disconnected from Solace")

    @contextmanager
    def session(self):
        self.connect()
        try:
            yield self
        finally:
            self.disconnect()


class AsyncSolaceClient:
    """
    Asynchronous Solace connection client.
    For async event loops and concurrent workflows.
    """

    def __init__(
        self,
        host: str = "tcp://localhost:55554",
        vpn: str = "default",
        username: str = "admin",
        password: str = "admin",
        client_name: str | None = None,
    ):
        self.host = host
        self.vpn = vpn
        self.username = username
        self.password = password
        self.client_name = client_name or f"solace-plug-async-{id(self)}"
        self._service: t.Optional[MessagingService] = None
        self._connected = False
        self._lock = asyncio.Lock()

        self._broker_props = {
            "solace.messaging.transport.host": host,
            "solace.messaging.service.vpn-name": vpn,
            "solace.messaging.authentication.scheme.basic.username": username,
            "solace.messaging.authentication.scheme.basic.password": password,
            "solace.messaging.client.name": self.client_name
        }

    @property
    def is_connected(self):
        return self._connected

    async def connect(self) -> None:
        """Asynchronously connect to Solace"""
        async with self._lock:
            if self._connected:
                return

            log.info("Connecting to Solace at %s (vpn=%s)...", self.host, self.vpn)
            loop = asyncio.get_running_loop()
            self._service = (
                MessagingService.builder()
                .from_properties(self._broker_props)
                .build()
            )
            await loop.run_in_executor(None, self._service.connect)
            self._connected = True
            log.info("Connected to Solace")

    async def disconnect(self) -> None:
        """Asynchronously disconnect from Solace"""
        async with self._lock:
            if not self._connected or not self._service:
                return

            log.info("Disconnecting from Solace...")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._service.disconnect)
            self._connected = False
            log.info("Disconnected from Solace")

    @asynccontextmanager
    async def session(self):
        await self.connect()
        try:
            yield self
        finally:
            await self.disconnect()
