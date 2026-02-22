"""Telegram client wrapper."""

import asyncio
import itertools
import json
import logging

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import SecretStr
from pydantic_settings import BaseSettings
from telethon import TelegramClient, hints, types  # type: ignore
from telethon.tl import custom, functions, patched  # type: ignore
from xdg_base_dirs import xdg_state_home

from mcp_telegram.types import (
    Dialog,
    DownloadedMedia,
    Media,
    Message,
    Messages,
)
from mcp_telegram.utils import get_unique_filename, parse_telegram_url

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Settings for the Telegram client."""

    api_id: str
    api_hash: SecretStr


class Telegram:
    """Wrapper around `telethon.TelegramClient` class."""

    def __init__(self):
        self._state_dir = xdg_state_home() / "mcp-telegram"
        self._state_dir.mkdir(parents=True, exist_ok=True)

        self._session_file = self._state_dir / "session"

        self._downloads_dir = self._state_dir / "downloads"
        self._downloads_dir.mkdir(parents=True, exist_ok=True)

        self._client: TelegramClient | None = None
        self._update_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._important_event: asyncio.Event = asyncio.Event()

        self._subscriptions_file = self._state_dir / "subscribed_chats.json"
        self._subscribed_chats: set[int] = set()
        self._last_active: datetime | None = None
        self._my_user_id: int | None = None
        self._my_username: str | None = None

    @property
    def client(self) -> TelegramClient:
        if self._client is None:
            raise RuntimeError("Client not created!")
        return self._client

    @property
    def session_file(self) -> Path:
        return self._session_file

    def create_client(
        self, api_id: str | None = None, api_hash: str | None = None
    ) -> TelegramClient:
        """Create a Telegram client.

        If `api_id` and `api_hash` are not provided, the client
        will use the default values from the `Settings` class.

        Args:
            api_id (`int`, optional): The API ID for the Telegram client.
            api_hash (`str`, optional): The API hash for the Telegram client.

        Returns:
            `telethon.TelegramClient`: The created Telegram client.

        Raises:
            `pydantic_core.ValidationError`: If `api_id` and `api_hash`
            are not provided.
        """
        if self._client is not None:
            return self._client

        settings: Settings
        if api_id is None or api_hash is None:
            settings = Settings()  # type: ignore
        else:
            settings = Settings(api_id=api_id, api_hash=SecretStr(api_hash))

        self._client = TelegramClient(
            session=self._session_file,
            api_id=int(settings.api_id),
            api_hash=settings.api_hash.get_secret_value(),
        )

        return self._client

    def _load_subscriptions(self) -> None:
        """Load subscribed chats and last_active timestamp from disk."""
        try:
            if self._subscriptions_file.exists():
                data = json.loads(self._subscriptions_file.read_text())
                self._subscribed_chats = set(int(c) for c in data.get("chats", []))
                last_active_str = data.get("last_active")
                if last_active_str:
                    self._last_active = datetime.fromisoformat(last_active_str)
        except Exception as e:
            logger.warning(f"Failed to load subscriptions: {e}")

    def _save_subscriptions(self) -> None:
        """Save subscribed chats and last_active timestamp to disk."""
        try:
            data = {
                "chats": list(self._subscribed_chats),
                "last_active": self._last_active.isoformat() if self._last_active else None,
            }
            self._subscriptions_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.warning(f"Failed to save subscriptions: {e}")

    async def register_event_handlers(self) -> None:
        """Register Telethon event handlers to buffer incoming updates.

        Also loads subscriptions from disk, resolves own user ID, and
        pre-fills the queue with any messages missed while the server was down.
        """
        from telethon import events as telethon_events
        from telethon.tl import types as tl_types

        client = self.client
        queue = self._update_queue
        important_event = self._important_event

        # Load persisted subscriptions and last_active timestamp
        self._load_subscriptions()

        # Resolve own user ID and username for mention detection
        me = await client.get_me()
        self._my_user_id = me.id  # type: ignore
        self._my_username = getattr(me, "username", None)

        # Catch up on missed messages from subscribed chats since last_active
        if self._last_active and self._subscribed_chats:
            logger.info(f"Catching up missed messages since {self._last_active.isoformat()}")
            for chat_id in list(self._subscribed_chats):
                try:
                    async for msg in client.iter_messages(chat_id, limit=50):
                        if msg.date is None or msg.date.tzinfo is None:
                            continue
                        msg_date = msg.date.replace(tzinfo=timezone.utc) if msg.date.tzinfo is None else msg.date
                        if msg_date <= self._last_active:
                            break
                        reply_to = msg.reply_to
                        thread_id = getattr(reply_to, "reply_to_top_id", None) if reply_to else None
                        reply_to_id = getattr(reply_to, "reply_to_msg_id", None) if reply_to else None
                        try:
                            queue.put_nowait({
                                "type": "new_message",
                                "timestamp": msg_date.isoformat(),
                                "chat_id": chat_id,
                                "message_id": msg.id,
                                "sender_id": getattr(msg, "sender_id", None),
                                "text": getattr(msg, "message", None) or "",
                                "outgoing": getattr(msg, "out", False),
                                "media_type": type(msg.media).__name__ if getattr(msg, "media", None) else None,
                                "reply_to": reply_to_id,
                                "thread_id": thread_id,
                                "missed": True,
                            })
                            important_event.set()
                        except asyncio.QueueFull:
                            pass
                except Exception as e:
                    logger.warning(f"Failed to catch up messages for chat {chat_id}: {e}")

        # Update last_active to now
        self._last_active = datetime.now(timezone.utc)
        self._save_subscriptions()

        def _enqueue(update: dict) -> None:
            try:
                queue.put_nowait(update)
            except asyncio.QueueFull:
                pass

            # Determine whether this update should wake a blocking get_pending_updates call.
            # Rules:
            #   new_message: wake if chat is subscribed OR the message mentions us
            #   message_edited: wake if chat is subscribed
            #   reactions/channel-deletes: wake if chat is subscribed (or chat unknown)
            #   DM deletes (UpdateDeleteMessages): always wake (no chat info available)
            #   typing, status, read receipts: never wake
            update_type = update["type"]
            chat_id = update.get("chat_id")

            should_wake = False
            if update_type == "new_message":
                should_wake = (
                    (chat_id is not None and chat_id in self._subscribed_chats)
                    or update.get("mentioned", False)
                )
            elif update_type == "message_edited":
                should_wake = chat_id is not None and chat_id in self._subscribed_chats
            elif update_type in ("UpdateMessageReactions", "UpdateDeleteChannelMessages"):
                # Wake if subscribed, or if we couldn't determine the chat
                should_wake = chat_id is None or chat_id in self._subscribed_chats
            elif update_type == "UpdateDeleteMessages":
                # No chat info in this update type — always wake (DM deletions)
                should_wake = True

            if should_wake:
                important_event.set()

        @client.on(telethon_events.NewMessage())
        async def _on_new_message(event):
            reply_to = event.message.reply_to
            thread_id = getattr(reply_to, "reply_to_top_id", None) if reply_to else None
            reply_to_id = getattr(reply_to, "reply_to_msg_id", None) if reply_to else None
            # Telethon sets message.mentioned=True if the current user was @-mentioned
            mentioned = getattr(event.message, "mentioned", False)
            _enqueue({
                "type": "new_message",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "chat_id": event.chat_id,
                "message_id": event.message.id,
                "sender_id": event.sender_id,
                "text": event.message.message or "",
                "outgoing": event.message.out,
                "media_type": type(event.message.media).__name__ if event.message.media else None,
                "reply_to": reply_to_id,
                "thread_id": thread_id,
                "mentioned": mentioned,
            })

        @client.on(telethon_events.MessageEdited())
        async def _on_message_edited(event):
            reply_to = event.message.reply_to
            thread_id = getattr(reply_to, "reply_to_top_id", None) if reply_to else None
            _enqueue({
                "type": "message_edited",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "chat_id": event.chat_id,
                "message_id": event.message.id,
                "sender_id": event.sender_id,
                "text": event.message.message or "",
                "media_type": type(event.message.media).__name__ if event.message.media else None,
                "thread_id": thread_id,
            })

        RAW_TYPES = (
            tl_types.UpdateMessageReactions,
            tl_types.UpdateDeleteMessages,
            tl_types.UpdateDeleteChannelMessages,
            tl_types.UpdateReadHistoryInbox,
            tl_types.UpdateReadHistoryOutbox,
            tl_types.UpdateUserTyping,
            tl_types.UpdateChatUserTyping,
            tl_types.UpdateUserStatus,
        )

        @client.on(telethon_events.Raw(RAW_TYPES))
        async def _on_raw_update(event):
            update_dict: dict = {
                "type": type(event).__name__,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": repr(event),
            }

            # Try to extract chat_id so subscription filtering can apply
            chat_id = None
            if hasattr(event, "peer") and event.peer is not None:
                try:
                    chat_id = await client.get_peer_id(event.peer)
                except Exception:
                    pass
            elif hasattr(event, "channel_id"):
                try:
                    chat_id = await client.get_peer_id(
                        tl_types.PeerChannel(channel_id=event.channel_id)
                    )
                except Exception:
                    pass

            if chat_id is not None:
                update_dict["chat_id"] = chat_id

            # Extract structured fields for reaction updates
            if isinstance(event, tl_types.UpdateMessageReactions):
                update_dict["message_id"] = event.msg_id
                reactions_out = []
                if event.reactions and event.reactions.results:
                    for rc in event.reactions.results:
                        r = rc.reaction
                        emoji = None
                        if isinstance(r, tl_types.ReactionEmoji):
                            emoji = r.emoticon
                        elif hasattr(r, "document_id"):
                            emoji = f"custom:{r.document_id}"
                        reactions_out.append({
                            "emoji": emoji,
                            "count": rc.count,
                            "chosen": rc.chosen_order is not None,
                        })
                update_dict["reactions"] = reactions_out

            _enqueue(update_dict)

    async def get_user_info(self, entity: str | int) -> dict:
        """Get profile info for a user, including bio and profile photo.

        Args:
            entity: Username, user ID, or phone number.

        Returns:
            dict with user details and optional profile_photo_path.
        """
        from telethon.tl import functions as tl_functions

        user = await self.client.get_entity(entity)
        full_result = await self.client(tl_functions.users.GetFullUserRequest(user))
        full_user = full_result.full_user

        status = user.status  # type: ignore
        status_str = type(status).__name__ if status else None

        info: dict = {
            "id": user.id,  # type: ignore
            "username": getattr(user, "username", None),
            "first_name": getattr(user, "first_name", None),
            "last_name": getattr(user, "last_name", None),
            "is_bot": getattr(user, "bot", False),
            "is_verified": getattr(user, "verified", False),
            "is_restricted": getattr(user, "restricted", False),
            "is_deleted": getattr(user, "deleted", False),
            "bio": getattr(full_user, "about", None),
            "status": status_str,
            "profile_photo_path": None,
        }

        # Download current profile photo if one exists
        if getattr(user, "photo", None):
            try:
                filename = f"profile_{user.id}.jpg"  # type: ignore
                filepath = self._downloads_dir / filename
                downloaded = await self.client.download_profile_photo(user, file=filepath)
                if downloaded:
                    info["profile_photo_path"] = str(filepath.resolve())
            except Exception as e:
                logger.warning(f"Failed to download profile photo for {entity}: {e}")

        return info

    async def get_group_info(self, entity: str | int) -> dict:
        """Get info about a group or channel, including admins.

        Args:
            entity: Username, group/channel ID, or invite link.

        Returns:
            dict with group details, description, member count, and admin list.
        """
        from telethon.tl import functions as tl_functions
        from telethon.tl.types import (
            Channel,
            ChannelParticipantsAdmins,
            Chat,
        )

        group = await self.client.get_entity(entity)
        info: dict = {}

        if isinstance(group, Channel):
            full_result = await self.client(
                tl_functions.channels.GetFullChannelRequest(group)
            )
            full_chat = full_result.full_chat
            info = {
                "id": group.id,
                "title": group.title,
                "username": getattr(group, "username", None),
                "type": "broadcast_channel" if group.broadcast else "supergroup",
                "description": getattr(full_chat, "about", None),
                "members_count": getattr(full_chat, "participants_count", None),
                "is_forum": getattr(group, "forum", False),
                "is_verified": getattr(group, "verified", False),
                "is_restricted": getattr(group, "restricted", False),
                "admins": [],
            }
            try:
                async for participant in self.client.iter_participants(
                    group, filter=ChannelParticipantsAdmins()
                ):
                    info["admins"].append({
                        "id": participant.id,
                        "username": getattr(participant, "username", None),
                        "first_name": getattr(participant, "first_name", None),
                        "last_name": getattr(participant, "last_name", None),
                    })
            except Exception as e:
                logger.warning(f"Failed to fetch admins for {entity}: {e}")

        elif isinstance(group, Chat):
            full_result = await self.client(
                tl_functions.messages.GetFullChatRequest(group.id)
            )
            full_chat = full_result.full_chat
            info = {
                "id": group.id,
                "title": group.title,
                "type": "group",
                "description": getattr(full_chat, "about", None),
                "members_count": getattr(group, "participants_count", None),
                "admins": [],
            }
            if full_result.participants:
                from telethon.tl.types import (
                    ChatParticipantAdmin,
                    ChatParticipantCreator,
                )
                users_by_id = {u.id: u for u in full_result.users}
                for p in full_result.participants.participants:
                    if isinstance(p, (ChatParticipantAdmin, ChatParticipantCreator)):
                        u = users_by_id.get(p.user_id)
                        if u:
                            info["admins"].append({
                                "id": u.id,
                                "username": getattr(u, "username", None),
                                "first_name": getattr(u, "first_name", None),
                                "last_name": getattr(u, "last_name", None),
                                "is_creator": isinstance(p, ChatParticipantCreator),
                            })
        else:
            info = {
                "id": getattr(group, "id", None),
                "title": getattr(group, "title", str(group)),
                "type": type(group).__name__,
            }

        return info

    async def subscribe_chat(self, entity: str | int) -> dict:
        """Subscribe to a chat so its messages wake the blocking get_pending_updates call.

        Args:
            entity: Username, chat ID, or phone number.

        Returns:
            dict with chat_id and subscribed=True.
        """
        peer = await self.client.get_entity(entity)
        chat_id = await self.client.get_peer_id(peer)
        self._subscribed_chats.add(chat_id)
        self._save_subscriptions()
        return {"chat_id": chat_id, "subscribed": True}

    async def unsubscribe_chat(self, entity: str | int) -> dict:
        """Unsubscribe from a chat.

        Args:
            entity: Username, chat ID, or phone number.

        Returns:
            dict with chat_id and subscribed=False.
        """
        peer = await self.client.get_entity(entity)
        chat_id = await self.client.get_peer_id(peer)
        self._subscribed_chats.discard(chat_id)
        self._save_subscriptions()
        return {"chat_id": chat_id, "subscribed": False}

    def get_subscribed_chats(self) -> list[int]:
        """Return the list of currently subscribed chat IDs."""
        return list(self._subscribed_chats)

    async def get_user_photos(
        self,
        entity: str | int,
        download_all: bool = False,
        download_index: int | None = None,
    ) -> list[dict]:
        """Get all profile photos for a user, with optional download.

        Args:
            entity: Username, user ID, or phone number.
            download_all: If True, download all photos to local files.
            download_index: If set, download only the photo at this index.

        Returns:
            list of dicts with index, photo_id, date, sizes, and optional path.
        """
        from telethon.tl import functions as tl_functions

        user = await self.client.get_entity(entity)

        result = await self.client(tl_functions.photos.GetUserPhotosRequest(
            user_id=user,
            offset=0,
            max_id=0,
            limit=100,
        ))

        output = []
        for i, photo in enumerate(result.photos):
            photo_info: dict = {
                "index": i,
                "photo_id": photo.id,
                "date": photo.date.isoformat() if getattr(photo, "date", None) else None,
                "sizes": [],
                "path": None,
            }

            if hasattr(photo, "sizes"):
                for s in photo.sizes:
                    size_info: dict = {"type": getattr(s, "type", None)}
                    if hasattr(s, "w"):
                        size_info["w"] = s.w
                    if hasattr(s, "h"):
                        size_info["h"] = s.h
                    if hasattr(s, "size"):
                        size_info["size"] = s.size
                    photo_info["sizes"].append(size_info)

            should_download = download_all or (download_index is not None and download_index == i)
            if should_download:
                try:
                    user_id = getattr(user, "id", str(entity))
                    filename = f"profile_{user_id}_{i}.jpg"
                    filepath = self._downloads_dir / filename
                    downloaded = await self.client.download_media(photo, file=filepath)
                    if downloaded:
                        photo_info["path"] = str(Path(downloaded).resolve())
                except Exception as e:
                    logger.warning(f"Failed to download photo {i} for {entity}: {e}")

            output.append(photo_info)

        return output

    async def get_forum_topics(self, entity: str | int, limit: int = 100) -> list[dict]:
        """Get the list of topics (threads) in a forum-enabled supergroup.

        Args:
            entity: Username or ID of the forum group.
            limit: Maximum number of topics to return. Defaults to 100.

        Returns:
            list of dicts, each containing topic id, title, top_message,
            date, is_closed, is_pinned, and is_hidden.

        Raises:
            ValueError: If the entity is not a forum group.
        """
        from telethon.tl import functions as tl_functions

        group = await self.client.get_entity(entity)

        result = await self.client(
            tl_functions.messages.GetForumTopicsRequest(
                peer=group,
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=limit,
                q=None,
            )
        )

        topics = []
        for topic in result.topics:
            topics.append({
                "id": topic.id,
                "title": topic.title,
                "top_message": topic.top_message,
                "date": topic.date.isoformat() if topic.date else None,
                "is_closed": getattr(topic, "closed", False),
                "is_hidden": getattr(topic, "hidden", False),
                "is_pinned": getattr(topic, "pinned", False),
            })

        return topics

    async def send_message(
        self,
        entity: str | int,
        message: str = "",
        file_path: list[str] | None = None,
        reply_to: int | None = None,
    ) -> None:
        """Send a message to a Telegram user, group, or channel.

        Args:
            entity (`str | int`): The recipient of the message.
            message (`str`, optional): The message to send.
            file_path (`list[str]`, optional): The list of paths to the files
                to be sent.
            reply_to (`int`, optional): The message ID to reply to.

        Raises:
            `FileNotFoundError`: If a file does not exist or is not a file.
        """

        if file_path:
            for path in file_path:
                _path = Path(path)
                if not _path.exists() or not _path.is_file():
                    logger.error(f"File {path} does not exist or is not a file.")
                    raise FileNotFoundError(
                        f"File {path} does not exist or is not a file."
                    )

        await self.client.send_message(
            entity,
            message,
            file=file_path,  # type: ignore
            reply_to=reply_to,  # type: ignore
        )

    async def edit_message(
        self, entity: str | int, message_id: int, message: str
    ) -> None:
        """Edit a message from a specific entity.

        Args:
            entity (`str | int`): The identifier of the entity.
            message_id (`int`): The ID of the message to edit.
            message (`str`): The message to edit the message to.
        """
        await self.client.edit_message(entity, message_id, message)

    async def delete_message(self, entity: str | int, message_ids: list[int]) -> None:
        """Delete a message from a specific entity.

        Args:
            entity (`str | int`): The identifier of the entity.
            message_ids (`list[int]`): The IDs of the messages to delete.
        """
        await self.client.delete_messages(entity, message_ids)

    async def get_draft(self, entity: str | int) -> str:
        """Get the draft message from a specific entity.

        Args:
            entity (`str | int`): The identifier of the entity.

        Returns:
            `str`: The draft message from the specific entity.
        """
        draft = await self.client.get_drafts(entity)

        assert isinstance(draft, custom.Draft)

        if isinstance(draft.text, str):  # type: ignore
            return draft.text

        return ""

    async def set_draft(self, entity: str | int, message: str) -> None:
        """Set a draft message for a specific entity.

        Args:
            entity (`str | int`): The identifier of the entity.
            message (`str`): The message to save as a draft.
        """

        peer_id = await self.client.get_peer_id(entity)
        draft = await self.client.get_drafts(peer_id)

        assert isinstance(draft, custom.Draft)

        await draft.set_message(message)  # type: ignore

    async def get_messages(
        self,
        entity: str | int,
        limit: int = 20,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        unread: bool = False,
        mark_as_read: bool = False,
    ) -> Messages:
        """Get messages from a specific entity.

        Args:
            entity (`str | int`):
                The entity to get messages from.
            limit (`int`, optional):
                The maximum number of messages to get. Defaults to 20.
            start_date (`datetime`, optional):
                The start date of the messages to get.
            end_date (`datetime`, optional):
                The end date of the messages to get.
            unread (`bool`, optional):
                Whether to get only unread messages. Defaults to False.
            mark_as_read (`bool`, optional):
                Whether to mark the messages as read. Defaults to False.

        Returns:
            `list[Message]`:
                A list of messages from the specific entity, ordered newest to oldest.
        """

        if end_date is None:
            end_date = datetime.now(timezone.utc)

        # make it very old if start_date is not provided
        if start_date is None:
            start_date = end_date - timedelta(days=10000)

        # make sure the dates are timezone-aware
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        _entity = await self.client.get_entity(entity)
        assert isinstance(_entity, hints.Entity)
        dialog = Dialog.from_entity(_entity)

        if unread:
            if not dialog or dialog.unread_messages_count == 0:
                return Messages(messages=[], dialog=dialog)
            limit = min(limit, dialog.unread_messages_count)

        results: list[Message] = []
        async for message in self.client.iter_messages(  # type: ignore
            _entity,
            offset_date=end_date,  # fetching messages older than end_date
        ):
            # Skip service messages and empty messages immediately
            if not isinstance(message, patched.Message) or isinstance(
                message, patched.MessageService | patched.MessageEmpty
            ):
                continue

            if message.date is None:
                continue

            if message.date < start_date or len(results) >= limit:
                break

            if mark_as_read:
                try:
                    await message.mark_read()
                except Exception as e:
                    logger.warning(f"Failed to mark message {message.id} as read: {e}")

            results.append(Message.from_message(message))

        return Messages(messages=results, dialog=dialog)

    async def download_media(
        self, entity: str | int, message_id: int, path: str | None = None
    ) -> DownloadedMedia:
        """Download media attached to a specific message to a unique local file.

        Args:
            entity (`str | int`): The chat/user where the message exists.
            message_id (`int`): The ID of the message containing the media.

        Returns:
            `DownloadedMedia`: An object containing the absolute path
                             and media details of the downloaded file.
        """

        # Fetch the specific message
        message = await self.client.get_messages(entity, ids=message_id)  # type: ignore

        if not message or not isinstance(message, patched.Message):
            raise ValueError(
                f"Message {message_id} not found or invalid in entity {entity}."
            )

        media = Media.from_message(message)
        if not media:
            raise ValueError(
                f"Message {message_id} in entity {entity} does not contain \
                    downloadable media."
            )

        filename = get_unique_filename(message)
        if path:
            filepath = Path(path) / filename
        else:
            filepath = self._downloads_dir / filename

        # Attempt to download the media to the specified file path
        try:
            downloaded_path = await message.download_media(file=filepath)  # type: ignore
        except Exception as e:
            logger.error(
                f"Error during media download for message {message_id} "
                f"in entity {entity}: {e}",
                exc_info=True,
            )
            raise e

        if downloaded_path and isinstance(downloaded_path, str):
            absolute_path = str(Path(downloaded_path).resolve())
            logger.info(
                f"Successfully downloaded media for message {message_id} \
                    to {absolute_path}."
            )
            return DownloadedMedia(path=absolute_path, media=media)

        raise ValueError(
            f"Failed to download media for message {message_id}. "
            f"download_media returned: {downloaded_path}"
        )

    async def message_from_link(self, link: str) -> Message:
        """Get a message from a link.

        Args:
            link (`str`): The link to get the message from.

        Returns:
            `Message`: The message from the link.

        Raises:
            `ValueError`: If the link is not a valid Telegram link.
        """

        # Parse the link to get the entity and message ID
        parsed_result = parse_telegram_url(link)

        if parsed_result is None:
            raise ValueError(
                f"Could not parse valid entity/message ID from link: {link}"
            )

        entity, message_id = parsed_result

        # Fetch the specific message using the parsed entity and ID
        message = await self.client.get_messages(entity, ids=message_id)  # type: ignore

        if not message or not isinstance(message, patched.Message):
            raise ValueError(
                f"Could not retrieve message {message_id} from entity {entity} \
                    (parsed from link: {link})"
            )

        return Message.from_message(message)

    async def _can_send_message(self, entity: hints.Entity) -> bool:
        """Check if the logged-in account can send messages to an entity.

        Args:
            entity (`hints.Entity`): The entity to check.

        Returns:
            `bool`: Whether the account can send messages to the entity.
        """

        if isinstance(entity, types.User):
            return True
        else:
            try:
                permissions = await self.client.get_permissions(entity, "me")
                assert isinstance(permissions, custom.ParticipantPermissions)

                if permissions.is_creator or (
                    permissions.is_admin and permissions.post_messages
                ):
                    return True

                if isinstance(entity, types.Channel) and entity.broadcast:
                    return False  # Regular members can't send to broadcast channels

                if permissions.is_banned:
                    assert isinstance(
                        permissions.participant,  # type: ignore
                        types.ChannelParticipantBanned,
                    )
                    return not permissions.participant.banned_rights.send_messages

                banned_rights = await self.client.get_permissions(entity)
                assert isinstance(banned_rights, types.ChatBannedRights)

                return not banned_rights.send_messages

            except Exception as e:
                logger.warning(f"Failed to get permissions for entity {entity}: {e}")
                return False

    async def search_dialogs(
        self, query: str, limit: int, global_search: bool = False
    ) -> list[Dialog]:
        """Search for users, groups, and channels globally.

        Args:
            query (`str`): The search query.
            limit (`int`): Maximum number of results to return.
            global_search (`bool`, optional): Whether to search globally.
                Defaults to False.

        Returns:
            `list[Dialog]`: A list of Dialog objects representing the search results.

        Raises:
            `ValueError`: If the query is empty or the limit is not greater than 0.
        """
        if not query:
            raise ValueError("Query cannot be empty!")

        if limit <= 0:
            raise ValueError("Limit must be greater than 0!")

        response: Any = await self.client(
            functions.contacts.SearchRequest(
                q=query,
                limit=limit,
            )
        )

        assert isinstance(response, types.contacts.Found)

        priority: dict[int, int] = {}
        for i, peer in enumerate(
            itertools.chain(response.my_results, response.results)
            if global_search
            else response.my_results
        ):
            peer_id = await self.client.get_peer_id(peer)
            priority[peer_id] = i

        result: list[Dialog] = []
        for x in itertools.chain(response.users, response.chats):
            if isinstance(x, hints.Entity):
                peer_id = await self.client.get_peer_id(x)
                if peer_id in priority:
                    can_send_message = await self._can_send_message(x)
                    try:
                        dialog = Dialog.from_entity(x, can_send_message)
                        result.append(dialog)
                    except Exception as e:
                        logger.warning(f"Failed to get dialog for entity {x.id}: {e}")

        # Sort results based on priority
        result.sort(key=lambda x: priority.get(x.id))  # type: ignore

        return result
