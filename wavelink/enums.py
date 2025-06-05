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

import enum


__all__ = ("NodeStatus", "TrackSource", "DiscordVoiceCloseType", "AutoPlayMode", "QueueMode")


class NodeStatus(enum.Enum):
    """Enum representing the connection status of a Node.

    Attributes
    ----------
    DISCONNECTED
        The Node has been disconnected or has never been connected previously.
    CONNECTING
        The Node is currently attempting to connect.
    CONNECTED
        The Node is currently connected.
    """

    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2


class TrackSource(enum.Enum):
    """Enum representing a :class:`Playable` source.

    Attributes
    ----------
    YouTube
        A source representing a track that comes from YouTube.
    YouTubeMusic
        A source representing a track that comes from YouTube Music.
    SoundCloud
        A source representing a track that comes from SoundCloud.
    """

    YouTube = 0
    YouTubeMusic = 1
    SoundCloud = 2
    AppleMusic = 3
    Deezer = 4
    YandexMusic = 5
    VkMusic = 6
    Tidal = 7
    Spotify = 8

class DiscordVoiceCloseType(enum.Enum):
    """Enum representing the various Discord Voice WebSocket Close Codes.

    These codes explain why the voice WebSocket connection was closed.
    Most are documented in Discord's developer docs, but some are newly observed.
    Clients should decide reconnect logic based on the specific code.

    Attributes
    ----------
    CLOSE_NORMAL : 1000
        Normal closure (standard WebSocket code).
    GOING_AWAY : 1001
        Connection is going away (e.g. browser tab closed).
    UNKNOWN_OPCODE : 4001
        An unknown opcode was sent.
    FAILED_DECODE_PAYLOAD : 4002
        Payload could not be decoded.
    NOT_AUTHENTICATED : 4003
        Client sent a message before identifying.
    AUTHENTICATION_FAILED : 4004
        Authentication failed.
    ALREADY_AUTHENTICATED : 4005
        Client sent multiple identify payloads.
    SESSION_INVALID : 4006
        Session is invalid.
    SESSION_TIMEOUT : 4009
        Session timed out.
    SERVER_NOT_FOUND : 4011
        The target voice server was not found.
    UNKNOWN_PROTOCOL : 4012
        An unsupported protocol was specified.
    DISCONNECTED : 4014
        Disconnect individual client (e.g. kicked, main session dropped). Should not reconnect.
    VOICE_SERVER_CRASHED : 4015
        Voice server crashed. Try resuming the session.
    UNKNOWN_ENCRYPTION_MODE : 4016
        Unrecognized encryption mode specified.
    BAD_REQUEST : 4020
        Malformed or bad voice payload. Undocumented.
    RATE_LIMIT_EXCEEDED : 4021
        Disconnected due to voice rate limit being exceeded. Should not reconnect. Undocumented.
    DISCONNECTED_ALL_CLIENTS : 4022
        Disconnects all clients (e.g. channel deleted, voice server changed).
        Should not reconnect. Behavior varies by context (e.g., channel deletion vs. server migration).
    """

    CLOSE_NORMAL = 1000
    GOING_AWAY = 1001
    UNKNOWN_OPCODE = 4001
    FAILED_DECODE_PAYLOAD = 4002
    NOT_AUTHENTICATED = 4003
    AUTHENTICATION_FAILED = 4004
    ALREADY_AUTHENTICATED = 4005
    SESSION_INVALID = 4006
    SESSION_TIMEOUT = 4009
    SERVER_NOT_FOUND = 4011
    UNKNOWN_PROTOCOL = 4012
    DISCONNECTED = 4014
    VOICE_SERVER_CRASHED = 4015
    UNKNOWN_ENCRYPTION_MODE = 4016
    BAD_REQUEST = 4020
    RATE_LIMIT_EXCEEDED = 4021
    DISCONNECTED_ALL_CLIENTS = 4022


class AutoPlayMode(enum.Enum):
    """Enum representing the various AutoPlay modes.

    Attributes
    ----------
    enabled
        When enabled, AutoPlay will work fully autonomously and fill the auto_queue with recommended tracks.
        If a song is put into a players standard queue, AutoPlay will use it as a priority.
    partial
        When partial, AutoPlay will work fully autonomously but **will not** fill the auto_queue with
        recommended tracks.
    disabled
        When disabled, AutoPlay will not do anything automatically.
    """

    enabled = 0
    partial = 1
    disabled = 2


class QueueMode(enum.Enum):
    """Enum representing the various modes on :class:`wavelink.Queue`

    Attributes
    ----------
    normal
        When set, the queue will not loop either track or history. This is the default.
    loop
        When set, the track will continuously loop.
    loop_all
        When set, the queue will continuously loop through all tracks.
    """

    normal = 0
    loop = 1
    loop_all = 2
