import asyncio
from contextlib import contextmanager, asynccontextmanager
import typing as t
from solace.messaging.messaging_service import MessagingService
from solace.messaging.errors.pubsubplus_client_error import IllegalStateError, PubSubPlusClientError
from .exceptions import ClientError, IllegalStateClientError, SolaceError
from .utils.decorators import retry_on_failure, retry_on_failure_async

from .log import log


class SolaceClient:
    """
    Synchronous Solace connection client.

    Provides blocking connect/disconnect operations for simple scripts
    and synchronous workflows.
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
            "solace.messaging.client.name": self.client_name,
        }

    @property
    def is_connected(self):
        return self._connected

    @retry_on_failure(exceptions=(IllegalStateClientError, ClientError))
    def connect(self) -> None:
        """
        Establish a connection to the Solace broker.

        Blocks until the connection completes.

        Raises:
            ClientError: If the broker rejects the connection.
            IllegalStateClientError: If another connect/disconnect is ongoing.
            SolaceError: For any other unexpected errors.
        """
        if self._connected:
            return

        try:
            self._service = (
                MessagingService.builder().from_properties(self._broker_props).build()
            )
            self._service.connect()

            if not self._service.is_connected:
                raise ClientError("Connection attempt failed")

            self._connected = True
            log.info("Connected to Solace at %s (vpn=%s)", self.host, self.vpn)
        except IllegalStateError as e:
            raise IllegalStateClientError(f"Illegal state during connect: {e}") from e
        except PubSubPlusClientError as e:
            raise ClientError(f"Broker rejected connection: {e}") from e
        except Exception as e:
            raise SolaceError(f"Unexpected failure: {e}") from e

    def disconnect(self):
        """
        Disconnect from the Solace broker.

        Safe to call even if not connected.
        """
        if not self._connected or not self._service:
            return

        log.info("Disconnecting from Solace...")
        self._service.disconnect()
        self._connected = False
        log.info("Disconnected from Solace")
        
    def get_messaging_service(self):
        """
        Get the underlying `MessagingService` instance.

        Raises:
            ClientError: If no connection is active.
        """
        if not self._connected:
            raise ClientError("Connect to Solace before using messaging service")
        return self._service

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
            "solace.messaging.client.name": self.client_name,
        }

    @property
    def is_connected(self):
        return self._connected

    @retry_on_failure_async(exceptions=(IllegalStateClientError, ClientError))
    async def connect(self) -> None:
        """
        Establish a connection to the Solace broker asynchronously.

        Runs the blocking `.connect()` in a thread executor to avoid blocking the loop.

        Raises:
            ClientError: If the broker rejects the connection.
            IllegalStateClientError: If another connect/disconnect is ongoing.
            SolaceError: For any other unexpected errors.
        """
        async with self._lock:
            if self._connected:
                return

            log.info("Connecting to Solace at %s (vpn=%s)...", self.host, self.vpn)
            loop = asyncio.get_running_loop()
            self._service = (
                MessagingService.builder().from_properties(self._broker_props).build()
            )

            try:
                await loop.run_in_executor(None, self._service.connect)

                if not self._service.is_connected:
                    raise ClientError("Connection attempt failed")

                self._connected = True
                log.info("Connected to Solace")

            except IllegalStateError as e:
                raise IllegalStateClientError(f"Illegal state during connect: {e}") from e
            except PubSubPlusClientError as e:
                raise ClientError(f"Broker rejected connection: {e}") from e
            except Exception as e:
                raise SolaceError(f"Unexpected failure: {e}") from e

    async def disconnect(self) -> None:
        """
        Disconnect from the Solace broker asynchronously.

        Safe to call even if not connected.
        """
        async with self._lock:
            if not self._connected or not self._service:
                return

            log.info("Disconnecting from Solace...")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._service.disconnect)
            self._connected = False
            log.info("Disconnected from Solace")

    def get_messaging_service(self):
        """
        Get the underlying `MessagingService` instance.

        Raises:
            ClientError: If no connection is active.
        """
        if not self._connected:
            raise ClientError("Connect to Solace before using messaging service")
        return self._service

    @asynccontextmanager
    async def session(self):
        await self.connect()
        try:
            yield self
        finally:
            await self.disconnect()
