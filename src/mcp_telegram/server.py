"""MCP Telegram Server."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from mcp.server.fastmcp import FastMCP

from mcp_telegram.telegram import Telegram
from mcp_telegram.types import Dialog, DownloadedMedia, Message, Messages
from mcp_telegram.utils import parse_entity


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[None]:
    """Lifespan manager for the app.

    This will connect to Telegram on startup and disconnect on shutdown.
    """
    try:
        tg.create_client()
        await tg.client.connect()
        await tg.register_event_handlers()
        yield
    finally:
        await tg.client.disconnect()  # type: ignore


tg = Telegram()
mcp = FastMCP(
    "mcp-telegram",
    lifespan=app_lifespan,
)


@mcp.tool()
async def send_message(
    entity: str,
    message: str = "",
    file_path: list[str] | None = None,
    reply_to: int | None = None,
) -> str:
    """Send a message to a Telegram user, group, or channel.

    It allows sending text messages to any Telegram entity identified by `entity`.

    !IMPORTANT: If you are not sure about the entity, use the `search_dialogs`
    tool and ask the user to select the correct entity from the list.

    Args:
        entity (`str`): The identifier of where to send the message.
            This can be a Telegram chat ID, a username, a phone number
            (in format '+1234567890'), or a group/channel username. The special
            value "me" can be used to send a message to yourself.

        message (`str`, optional): The text message to be sent.
            The message supports Markdown formatting including **bold**, __italic__,
            `monospace`, and [URL](links). The maximum length for a message is 35,000
            bytes or 4,096 characters.

        file_path (`list[str]`, optional): The list of paths to the files to be sent.

        reply_to (`int`, optional): The message ID to reply to.

    Returns:
        `str`:
            A success message if sent, or an error message if failed.
    """

    _entity = parse_entity(entity)

    await tg.send_message(
        _entity,
        message,
        file_path=file_path,
        reply_to=reply_to,
    )

    return f"Message sent to {entity}"


@mcp.tool()
async def edit_message(entity: str, message_id: int, message: str) -> str:
    """Edit a message from a specific entity.

    Edits a message from a specific entity.

    !IMPORTANT: If the entity is not found, it will return an error message.
    If you are not sure about the entity, use the `search_dialogs`
    tool and ask the user to select the correct entity from the list.
    If you are not sure about the message ID, use the `get_messages`
    tool to get the message ID.

    Args:
        entity (`str`): The identifier of the entity.
        message_id (`int`): The ID of the message to edit.
        message (`str`): The message to edit the message to.

    Returns:
        `str`:
            A success message if edited, or an error message if failed.
    """

    _entity = parse_entity(entity)

    await tg.edit_message(_entity, message_id, message)

    return f"Message edited in {entity}"


@mcp.tool()
async def delete_message(entity: str, message_ids: list[int]) -> str:
    """Delete messages from a specific entity.

    Deletes messages from a specific entity.

    !IMPORTANT: If the entity is not found, it will return an error message.
    If you are not sure about the entity, use the `search_dialogs`
    tool and ask the user to select the correct entity from the list.
    If you are not sure about the message IDs, use the `get_messages`
    tool to get the message IDs.

    Args:
        entity (`str`): The identifier of the entity.
        message_ids (`list[int]`): The IDs of the messages to delete.

    Returns:
        `str`:
            A success message if deleted, or an error message if failed.
    """

    _entity = parse_entity(entity)

    await tg.delete_message(_entity, message_ids)

    return f"Messages deleted from {entity}"


@mcp.tool()
async def search_dialogs(
    query: str, limit: int = 10, global_search: bool = False
) -> list[Dialog]:
    """Search for users, groups, and channels.

    Retrieves users, groups, and channels and filters them based
    on the provided query. The query performs a case-insensitive search.

    !IMPORTANT: If the query doesn't return the correct results, it means that
    the query is not specific enough. Try to be more specific with the query or
    use a different query.

    Args:
        query (`str`): A query string to filter the dialogs.
            The search will return only dialogs where the query string is
            found within the dialog's title or username.

        limit (`int`, optional): The maximum number of dialogs to return.
            Defaults to 10. The limit must be greater than 0.

        global_search (`bool`, optional): Whether to perform a global search.
            Defaults to False.

    Returns:
        `list[Dialog]`: A list of dialogs that match the query if successful,
            or an error message if request failed.
    """

    return await tg.search_dialogs(query, limit, global_search)


@mcp.tool()
async def get_draft(entity: str) -> str:
    """Get the draft message for a specific entity.

    Finds the draft message for an entity specified by username, chat_id,
    phone number, or 'me'.

    !IMPORTANT: If the entity is not found, it will return an error message.
    If you are not sure about the entity, use the `search_dialogs`
    tool and ask the user to select the correct entity from the list.

    Args:
        entity (`str`):
            The identifier of the entity to get the draft message for.
            This can be a Telegram chat ID, a username, a phone number, or 'me'.

    Returns:
        `str`:
            The draft message (empty string if no draft) for the specific entity
            or an error message if request failed.
    """

    _entity = parse_entity(entity)

    return await tg.get_draft(_entity)


@mcp.tool()
async def set_draft(entity: str, message: str) -> str:
    """Set a draft message for a specific entity.

    Sets a draft message for an entity specified by username, chat_id,
    phone number, or 'me'.

    !IMPORTANT: If the entity is not found, it will return an error message.
    If you are not sure about the entity, use the `search_dialogs`
    tool and ask the user to select the correct entity from the list.

    Args:
        entity (`str`):
            The identifier of the entity to save the draft message for.
            This can be a Telegram chat ID, a username, a phone number, or 'me'.

        message (`str`):
            The message to save as a draft.

    Returns:
        `str`:
            A success message if saved, or an error message if failed.
    """

    _entity = parse_entity(entity)

    await tg.set_draft(_entity, message)

    return f"Draft saved for {_entity}"


@mcp.tool()
async def get_messages(
    entity: str,
    limit: int = 10,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    unread: bool = False,
    mark_as_read: bool = False,
) -> Messages:
    """Get messages from a specific entity.

    Retrieves messages from an entity specified by username, chat_id,
    phone number, or 'me'.

    !IMPORTANT: If the entity is not found, it will return an error message.
    If you are not sure about the entity, use the `search_dialogs`
    tool and ask the user to select the correct entity from the list.

    Args:
        entity (`str`):
            The identifier of the entity to get messages from.
            This can be a Telegram chat ID, a username, a phone number, or 'me'.

        limit (`int`, optional):
            The maximum number of messages to retrieve.
            Defaults to 10.

        start_date (`datetime`, optional):
            The start date of the messages to retrieve.

        end_date (`datetime`, optional):
            The end date of the messages to retrieve.

        unread (`bool`, optional):
            Whether to get only unread messages.
            Defaults to False.

        mark_as_read (`bool`, optional):
            Whether to mark the messages as read.
            Defaults to False.

    Returns:
        `Messages`:
            A list of messages from the entity and the dialog the messages
            belong to if successful, or an error message if request failed.
    """

    _entity = parse_entity(entity)

    return await tg.get_messages(
        _entity,
        limit,
        start_date,
        end_date,
        unread,
        mark_as_read,
    )


@mcp.tool()
async def media_download(
    entity: str, message_id: int, path: str | None = None
) -> DownloadedMedia:
    """Download media from a specific message to a unique local file.

    Retrieves media from an entity specified by username, chat_id,
    phone number, or 'me' and saves it to a local directory with a unique name.

    !IMPORTANT: If the entity is not found, it will return an error message.
    If you are not sure about the entity, use the `search_dialogs`
    tool and ask the user to select the correct entity from the list.

    Args:
        entity (`str`):
            The identifier of the entity where the message exists.
            This can be a Telegram chat ID, a username, a phone number, or 'me'.

        message_id (`int`):
            The ID of the message containing the media to download.

        path (`str`, optional):
            The path to save the downloaded media.
            Defaults to a Path corresponding to `XDG_STATE_HOME`.

    Returns:
        `DownloadedMedia`:
            An object containing the absolute path and media details
            of the downloaded file if successful or an error message.
    """
    _entity = parse_entity(entity)

    return await tg.download_media(_entity, message_id, path)


@mcp.tool()
async def message_from_link(link: str) -> Message:
    """Get a message from a link.

    Retrieves a message from a link.

    !IMPORTANT: If the link is not a valid Telegram message link, or the account
    is not authorized to access the message, it will return an error message.

    Args:
        link (`str`): The link to the message.

    Returns:
        `Message`: The message from the link if successful, or an error message.
    """

    return await tg.message_from_link(link)


@mcp.tool()
async def get_pending_updates(
    max_count: int = 100,
    timeout: float | None = None,
    min_wait: float | None = None,
) -> list[dict]:
    """Drain and return queued Telegram updates since last poll.

    With timeout set, blocks until at least one update arrives or timeout expires.
    Without timeout, returns immediately (empty list if no updates queued).

    Args:
        max_count: Maximum number of updates to return. Defaults to 100.
        timeout: Seconds to wait for at least one update. None = return immediately.
        min_wait: Minimum seconds to wait before returning, even if an important
            event arrives early. Useful for batching rapid messages. None = no minimum.

    Returns:
        list[dict]: List of update dicts (new_message, message_edited, reactions,
            deletes, read receipts, typing, status), ordered oldest-first.
    """
    results: list[dict] = []
    start = asyncio.get_event_loop().time()

    if timeout is not None:
        try:
            # Block until an important event arrives (new message, reaction, delete, edit)
            # Typing and status events do NOT unblock this wait
            await asyncio.wait_for(tg._important_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        else:
            tg._important_event.clear()

    # Enforce minimum wait time after waking (lets rapid follow-up messages accumulate)
    if min_wait is not None:
        elapsed = asyncio.get_event_loop().time() - start
        remaining = min_wait - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)

    # Drain all queued updates (important and non-important alike)
    while not tg._update_queue.empty() and len(results) < max_count:
        try:
            results.append(tg._update_queue.get_nowait())
        except asyncio.QueueEmpty:
            break

    # Update last_active so crash recovery knows when we last checked
    tg._last_active = datetime.now(timezone.utc)
    tg._save_subscriptions()

    return results


@mcp.tool()
async def subscribe_chat(entity: str) -> dict:
    """Subscribe to a chat so its messages immediately wake the get_pending_updates loop.

    Subscribed chats also have their missed messages fetched automatically on MCP restart.
    Subscription list is persisted to disk and survives server restarts.

    Args:
        entity (`str`): Username, chat ID, or phone number to subscribe to.

    Returns:
        `dict`: chat_id and subscribed=True on success.
    """
    _entity = parse_entity(entity)
    return await tg.subscribe_chat(_entity)


@mcp.tool()
async def unsubscribe_chat(entity: str) -> dict:
    """Unsubscribe from a chat. Messages from it will still queue but won't wake the loop.

    Args:
        entity (`str`): Username, chat ID, or phone number to unsubscribe from.

    Returns:
        `dict`: chat_id and subscribed=False on success.
    """
    _entity = parse_entity(entity)
    return await tg.unsubscribe_chat(_entity)


@mcp.tool()
def list_subscribed_chats() -> list[int]:
    """Return the list of currently subscribed chat IDs.

    Returns:
        `list[int]`: List of subscribed chat IDs. Messages from these chats
            will immediately wake a blocking get_pending_updates call.
    """
    return tg.get_subscribed_chats()


@mcp.tool()
async def get_forum_topics(entity: str, limit: int = 100) -> list[dict]:
    """Get the list of topics (threads) in a forum-enabled Telegram group.

    Only works on supergroups with the forum feature enabled.

    Args:
        entity (`str`): Username or ID of the forum group.
        limit (`int`, optional): Maximum number of topics to return. Defaults to 100.

    Returns:
        `list[dict]`: List of topics, each with id, title, top_message,
            date, is_closed, is_pinned, and is_hidden.
    """
    _entity = parse_entity(entity)
    return await tg.get_forum_topics(_entity, limit)


@mcp.tool()
async def get_user_photos(
    entity: str,
    download_all: bool = False,
    download_index: int | None = None,
) -> list[dict]:
    """Get all profile photos for a Telegram user.

    Returns metadata for all profile photos, with optional download of any or all.

    Args:
        entity (`str`): Username, user ID, or phone number of the user.
        download_all (`bool`, optional): If True, download all photos to local files.
            Defaults to False.
        download_index (`int`, optional): If set, download only the photo at this
            index (0 = most recent). Defaults to None (no download).

    Returns:
        `list[dict]`: List of photo dicts, each with index, photo_id, date,
            sizes (list of {type, w, h, size}), and path (if downloaded).
    """
    _entity = parse_entity(entity)
    return await tg.get_user_photos(_entity, download_all, download_index)


@mcp.tool()
async def get_user_info(entity: str) -> dict:
    """Get profile information for a Telegram user.

    Returns user details including bio, verification status, and downloads
    their current profile photo to a local file if one exists.

    Args:
        entity (`str`): Username, user ID, or phone number of the user.

    Returns:
        `dict`: User info including id, username, first_name, last_name, bio,
            is_bot, is_verified, status, and profile_photo_path (if available).
    """
    _entity = parse_entity(entity)
    return await tg.get_user_info(_entity)


@mcp.tool()
async def get_group_info(entity: str) -> dict:
    """Get information about a Telegram group or channel.

    Returns group details including description, member count, and admin list.
    Works with regular groups, supergroups, and broadcast channels.

    Args:
        entity (`str`): Username, group/channel ID, or invite link.

    Returns:
        `dict`: Group info including id, title, type, description, members_count,
            is_forum, is_verified, and admins list.
    """
    _entity = parse_entity(entity)
    return await tg.get_group_info(_entity)
