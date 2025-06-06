"""
MIT License

Copyright (c) 2019-Current PythonistaGuild, EvieePy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import aiohttp

from . import __version__
from .backoff import Backoff
from .enums import NodeStatus
from .exceptions import AuthorizationFailedException, NodeException
from .payloads import *
from .tracks import Playable


if TYPE_CHECKING:
    from .node import Node
    from .player import Player
    from .types.request import UpdateSessionRequest
    from .types.response import InfoResponse
    from .types.state import PlayerState
    from .types.websocket import TrackExceptionPayload, WebsocketOP


logger: logging.Logger = logging.getLogger(__name__)
LOGGER_TRACK: logging.Logger = logging.getLogger("TrackException")


class Websocket:
    def __init__(self, *, node: Node) -> None:
        self.node = node

        self.backoff: Backoff = Backoff()

        self.socket: aiohttp.ClientWebSocketResponse | None = None
        self.keep_alive_task: asyncio.Task[None] | None = None

    @property
    def headers(self) -> dict[str, str]:
        assert self.node.client is not None
        assert self.node.client.user is not None

        data = {
            "Authorization": self.node.password,
            "User-Id": str(self.node.client.user.id),
            "Client-Name": f"Wavelink/{__version__}",
        }

        if self.node.session_id:
            data["Session-Id"] = self.node.session_id

        return data

    def is_connected(self) -> bool:
        return self.socket is not None and not self.socket.closed

    async def _update_node(self) -> None:
        if self.node._resume_timeout > 0:
            udata: UpdateSessionRequest = {"resuming": True, "timeout": self.node._resume_timeout}
            await self.node._update_session(data=udata)

        info: InfoResponse = await self.node._fetch_info()
        if "spotify" in info["sourceManagers"]:
            self.node._spotify_enabled = True

    async def connect(self) -> None:
        if self.node._status is NodeStatus.CONNECTED:
            # Node was previously connected...
            # We can dispatch an event to say the node was disconnected...
            payload: NodeDisconnectedEventPayload = NodeDisconnectedEventPayload(node=self.node)
            self.dispatch("node_disconnected", payload)

        self.node._status = NodeStatus.CONNECTING

        if self.keep_alive_task:
            try:
                self.keep_alive_task.cancel()
            except Exception as e:
                logger.debug(
                    "Failed to cancel websocket keep alive while connecting. This is most likely not a problem and will not affect websocket connection: '%s'",
                    e,
                )

        retries: int | None = self.node._retries
        session: aiohttp.ClientSession = self.node._session
        heartbeat: float = self.node.heartbeat
        uri: str = f"{self.node.uri.removesuffix('/')}/v4/websocket"
        github: str = "https://github.com/PythonistaGuild/Wavelink/issues"
        current_attempt = 0
        initial_retries = self.node._retries
        total_retries_display = str(initial_retries) if initial_retries is not None else '∞'

        while True:
            try:
                current_attempt += 1
                self.socket = await session.ws_connect(url=uri, heartbeat=heartbeat, headers=self.headers)  # type: ignore
            except Exception as e:
                if isinstance(e, aiohttp.WSServerHandshakeError) and e.status == 401:
                    await self.cleanup()
                    raise AuthorizationFailedException from e
                elif isinstance(e, aiohttp.WSServerHandshakeError) and e.status == 404:
                    await self.cleanup()
                    raise NodeException from e
                else:
                    logger.warning(
                        'An unexpected error occurred while connecting %r to Lavalink: "%s"\nIf this error persists or wavelink is unable to reconnect, please see: %s',
                        self.node,
                        e,
                        github,
                    )

            if self.is_connected():
                self.keep_alive_task = asyncio.create_task(self.keep_alive())
                break

            if retries == 0:
                logger.error(
                    '%r was unable to successfully connect/reconnect to Lavalink after "%s" connection attempt. This Node has exhausted the retry count.',
                    self.node,
                    retries + 1,
                )

                await self.cleanup()
                break

            if retries:
                retries -= 1

            delay: float
            if self.node._retry_interval is not None:
                delay = self.node._retry_interval
            else:
                # Fallback if not set
                delay = self.backoff.calculate()
            logger.info('%r (Attempt %s/%s) retrying websocket connection in "%s" seconds.', self.node, current_attempt, total_retries_display, delay)

            await asyncio.sleep(delay)

    async def keep_alive(self) -> None:
        """The main websocket keep-alive loop."""
        assert self.socket is not None

        while not self.socket.closed:
            try:
                message: aiohttp.WSMessage = await self.socket.receive()

                # --- CRITICAL FIX: Handle ALL close message types first ---
                # This now includes WSMsgType.CLOSE, which carries the 1001 code.
                if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    logger.warning(
                        "Websocket connection to %r is closing or has been closed (code: %s, reason: '%s'). Attempting to reconnect...",
                        self.node,
                        message.data,  # This will contain the close code (e.g., 1001)
                        message.extra
                    )
                    asyncio.create_task(self.connect())
                    break  # Exit the loop, this task is done.

            except asyncio.CancelledError:
                logger.debug("Keep-alive task for node %r was cancelled.", self.node)
                break
            except (aiohttp.ClientError, aiohttp.ClientPayloadError) as e:
                # This handles abrupt connection loss (hard disconnects, RTO).
                logger.error("Websocket connection to %r lost: %s. Attempting to reconnect...", self.node, e)
                if not self.socket.closed:
                    await self.socket.close(code=4000) # Use a non-standard code to indicate an abnormal closure
                asyncio.create_task(self.connect())
                break

            # If we are here, the message is not a close signal and the connection is alive.
            # Now, we process it as a data message.
            try:
                if message.data is None:
                    continue

                try:
                    data: WebsocketOP = message.json()
                except (TypeError, ValueError):
                    logger.error("Failed to parse JSON message from Lavalink. Data: %s", message.data)
                    continue

                op = data.get("op")
                logger.debug("Received OP '%s' from node %r", op, self.node)

                if op == "ready":
                    self.node._status = NodeStatus.CONNECTED
                    self.node._session_id = data["sessionId"]
                    await self._update_node()
                    payload = NodeReadyEventPayload(node=self.node, resumed=data["resumed"], session_id=data["sessionId"])
                    self.dispatch("node_ready", payload)

                elif op == "playerUpdate":
                    player = self.get_player(data["guildId"])
                    payload = PlayerUpdateEventPayload(player=player, state=data["state"])
                    self.dispatch("player_update", payload)
                    if player:
                        asyncio.create_task(player._update_event(payload))

                elif op == "stats":
                    payload = StatsEventPayload(data=data)
                    self.node._total_player_count = payload.players
                    self.dispatch("stats_update", payload)

                elif op == "event":
                    player: Player | None = self.get_player(data["guildId"])
                    event_type = data.get("type")

                    if event_type == "TrackStartEvent":
                        track = Playable(data["track"])
                        payload = TrackStartEventPayload(player=player, track=track)
                        self.dispatch("track_start", payload)
                        if player:
                            asyncio.create_task(player._track_start(payload))

                    elif event_type == "TrackEndEvent":
                        track = Playable(data["track"])
                        reason = data["reason"]
                        if player and reason != "replaced":
                            player._current = None
                        payload = TrackEndEventPayload(player=player, track=track, reason=reason)
                        self.dispatch("track_end", payload)
                        if player:
                            asyncio.create_task(player._auto_play_event(payload))

                    elif event_type == "TrackExceptionEvent":
                        track = Playable(data["track"])
                        exception = data["exception"]
                        payload = TrackExceptionEventPayload(player=player, track=track, exception=exception)
                        LOGGER_TRACK.error(
                            "A Lavalink TrackException was received on %r for player %r: %s, caused by: %s, with severity: %s",
                            self.node, player, exception.get("message", ""), exception["cause"], exception["severity"],
                        )
                        self.dispatch("track_exception", payload)

                    elif event_type == "TrackStuckEvent":
                        track = Playable(data["track"])
                        threshold = data["thresholdMs"]
                        payload = TrackStuckEventPayload(player=player, track=track, threshold=threshold)
                        self.dispatch("track_stuck", payload)

                    elif event_type == "WebSocketClosedEvent":
                        code, reason, by_remote = data["code"], data["reason"], data["byRemote"]
                        payload = WebsocketClosedEventPayload(player=player, code=code, reason=reason, by_remote=by_remote)
                        self.dispatch("websocket_closed", payload)
                        if player:
                            asyncio.create_task(player._disconnected_wait(code, by_remote))
                    
                    else:
                        payload = ExtraEventPayload(node=self.node, player=player, data=data)
                        self.dispatch("extra_event", payload)

                else:
                    logger.warning("Received unknown OP from Lavalink: '%s'. Disregarding.", data.get("op"))

            except Exception as e:
                logger.exception("An unexpected error occurred while processing a Lavalink message: %s", e)

        logger.debug("Keep-alive task for node %r has finished and exited the loop.", self.node)

    def get_player(self, guild_id: str | int) -> Player | None:
        return self.node.get_player(int(guild_id))

    def dispatch(self, event: str, /, *args: Any, **kwargs: Any) -> None:
        assert self.node.client is not None

        self.node.client.dispatch(f"wavelink_{event}", *args, **kwargs)
        logger.debug("%r dispatched the event 'on_wavelink_%s'", self.node, event)

    async def cleanup(self) -> None:
        if self.keep_alive_task:
            try:
                self.keep_alive_task.cancel()
            except Exception:
                pass

        if self.socket:
            try:
                await self.socket.close()
            except Exception:
                pass

        self.node._status = NodeStatus.DISCONNECTED
        self.node._session_id = None
        self.node._players = {}

        self.node._websocket = None

        payload: NodeDisconnectedEventPayload = NodeDisconnectedEventPayload(node=self.node)
        self.dispatch("node_disconnected", payload)

        logger.debug("Successfully cleaned up the websocket for %r", self.node)
