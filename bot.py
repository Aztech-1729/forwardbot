import asyncio
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# Allow running this file from any working directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.exceptions import TelegramBadRequest
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ServerSelectionTimeoutError
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
    UserBannedInChannelError,
)
from telethon.sessions import StringSession

import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("forward_bot")


class MongoStore:
    """MongoDB-backed storage. No in-memory state for critical flows."""

    def __init__(self) -> None:
        # Fail fast on Mongo issues; bot cannot operate without MongoDB.
        self._client = AsyncIOMotorClient(
            config.MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )
        self.db = self._client[config.MONGO_DB_NAME]

        # Required collections
        self.account = self.db["account_status"]
        self.source = self.db["source_group"]
        self.targets = self.db["target_groups"]
        self.forwarding = self.db["forwarding_state"]

        # Per-admin UI state and dashboard message pointers are stored inside
        # the required `forwarding_state` collection (single document _id="state").

    async def ensure_defaults(self) -> None:
        await self.account.update_one(
            {"_id": "shared"},
            {"$setOnInsert": {"connected": False, "string_session": None, "login": None}},
            upsert=True,
        )
        await self.forwarding.update_one(
            {"_id": "state"},
            {"$setOnInsert": {"running": False, "mode": "forward", "ui": {}, "dashboards": {}}},
            upsert=True,
        )

    async def set_dashboard_message(self, admin_id: int, chat_id: int, message_id: int) -> None:
        await self.forwarding.update_one(
            {"_id": "state"},
            {"$set": {f"dashboards.{admin_id}": {"chat_id": chat_id, "message_id": message_id}}},
            upsert=True,
        )

    async def get_dashboard_message(self, admin_id: int) -> Optional[Dict[str, Any]]:
        doc = await self.forwarding.find_one({"_id": "state"})
        dashboards = (doc or {}).get("dashboards", {})
        return dashboards.get(str(admin_id)) or dashboards.get(admin_id)

    async def set_ui_state(self, admin_id: int, state: Optional[Dict[str, Any]]) -> None:
        if state is None:
            await self.forwarding.update_one(
                {"_id": "state"}, {"$unset": {f"ui.{admin_id}": ""}}, upsert=True
            )
        else:
            await self.forwarding.update_one(
                {"_id": "state"}, {"$set": {f"ui.{admin_id}": state}}, upsert=True
            )

    async def get_ui_state(self, admin_id: int) -> Optional[Dict[str, Any]]:
        doc = await self.forwarding.find_one({"_id": "state"})
        ui = (doc or {}).get("ui", {})
        return ui.get(str(admin_id)) or ui.get(admin_id)

    async def get_account(self) -> Dict[str, Any]:
        doc = await self.account.find_one({"_id": "shared"})
        return doc or {"_id": "shared", "connected": False, "string_session": None, "login": None}

    async def set_account_connected(self, connected: bool, string_session: Optional[str]) -> None:
        await self.account.update_one(
            {"_id": "shared"},
            {"$set": {"connected": connected, "string_session": string_session, "login": None}},
            upsert=True,
        )

    async def logout_and_reset(self) -> None:
        """Disconnect user session and reset all configured groups/state.

        Note: we keep `dashboards` so existing dashboard messages can still be edited.
        """
        await self.set_account_connected(False, None)
        await self.clear_source()
        await self.clear_targets()
        # Reset forwarding state and clear per-admin UI flows.
        await self.forwarding.update_one(
            {"_id": "state"},
            {"$set": {"running": False, "ui": {}}},
            upsert=True,
        )

    async def set_account_login(self, login: Optional[Dict[str, Any]]) -> None:
        await self.account.update_one({"_id": "shared"}, {"$set": {"login": login}}, upsert=True)

    async def set_source(self, group_id: int, title: str) -> None:
        await self.source.update_one(
            {"_id": "source"}, {"$set": {"group_id": group_id, "title": title}}, upsert=True
        )

    async def get_source(self) -> Optional[Dict[str, Any]]:
        return await self.source.find_one({"_id": "source"})

    async def clear_source(self) -> None:
        await self.source.delete_one({"_id": "source"})

    async def upsert_target(self, group_id: int, title: str, enabled: bool = True) -> None:
        await self.targets.update_one(
            {"_id": group_id},
            {"$set": {"group_id": group_id, "title": title, "enabled": enabled}},
            upsert=True,
        )

    async def list_targets(self) -> List[Dict[str, Any]]:
        cursor = self.targets.find({}).sort("title", 1)
        return [doc async for doc in cursor]

    async def get_target(self, group_id: int) -> Optional[Dict[str, Any]]:
        return await self.targets.find_one({"_id": group_id})

    async def set_target_enabled(self, group_id: int, enabled: bool) -> None:
        await self.targets.update_one({"_id": group_id}, {"$set": {"enabled": enabled}})

    async def clear_targets(self) -> None:
        await self.targets.delete_many({})

    async def remove_target(self, group_id: int) -> None:
        await self.targets.delete_one({"_id": group_id})

    async def set_running(self, running: bool) -> None:
        await self.forwarding.update_one({"_id": "state"}, {"$set": {"running": running}}, upsert=True)

    async def get_running(self) -> bool:
        doc = await self.forwarding.find_one({"_id": "state"})
        return bool(doc and doc.get("running"))

    async def get_mode(self) -> str:
        doc = await self.forwarding.find_one({"_id": "state"})
        mode = (doc or {}).get("mode") or "forward"
        return mode if mode in {"forward", "delete"} else "forward"

    async def set_mode(self, mode: str) -> None:
        if mode not in {"forward", "delete"}:
            mode = "forward"
        await self.forwarding.update_one({"_id": "state"}, {"$set": {"mode": mode}}, upsert=True)


class SharedTelethon:
    """Single shared Telegram user account for all admins (Telethon)."""

    def __init__(self, store: MongoStore) -> None:
        self.store = store
        self._client: Optional[TelegramClient] = None

    async def _build_client(self) -> TelegramClient:
        account = await self.store.get_account()
        ss = account.get("string_session")
        session = StringSession(ss) if ss else StringSession()
        # Avoid creating session files; StringSession is stored in MongoDB.
        return TelegramClient(session, config.API_ID, config.API_HASH)

    async def get_client(self) -> TelegramClient:
        if self._client is None:
            self._client = await self._build_client()
        if not self._client.is_connected():
            await self._client.connect()
        return self._client

    async def is_authorized(self) -> bool:
        client = await self.get_client()
        return await client.is_user_authorized()

    async def export_string_session(self) -> str:
        client = await self.get_client()
        return client.session.save()

    async def reset_client(self) -> None:
        """Disconnect and drop the in-memory Telethon client so next use rebuilds from Mongo."""
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                pass
        self._client = None

    async def resolve_chat(self, chat_ref: str):
        """Resolve a group/channel by numeric ID.

        Admins often copy IDs in different forms:
        - Basic groups: negative IDs like -123456789
        - Supergroups/channels: IDs like -1001234567890
        Sometimes users paste the channel id without -100 prefix; we retry as -100<ID>.
        """
        ref = chat_ref.strip()
        if not re.fullmatch(r"-?\d+", ref):
            raise ValueError("Group ID must be numeric")

        raw_id = int(ref)
        client = await self.get_client()

        try:
            return await client.get_entity(raw_id)
        except Exception as first_exc:  # noqa: BLE001
            # Retry common case: user pasted channel_id without -100 prefix
            if raw_id > 0:
                try:
                    return await client.get_entity(int(f"-100{raw_id}"))
                except Exception:  # noqa: BLE001
                    pass
            raise first_exc

    async def ensure_admin(self, entity) -> Tuple[bool, str]:
        client = await self.get_client()
        try:
            me = await client.get_me()
            perms = await client.get_permissions(entity, me)
            if getattr(perms, "is_admin", False) or getattr(perms, "is_creator", False):
                return True, ""
            return False, "❌ Add account as admin in this group"
        except Exception as e:  # noqa: BLE001
            logger.exception("Failed permission check")
            return False, f"❌ Permission check failed: {type(e).__name__}"


def is_admin(user_id: int) -> bool:
    # Be tolerant to config values accidentally being strings.
    try:
        admin_ids = {int(x) for x in (config.ADMIN_IDS or [])}
    except Exception:
        admin_ids = set(config.ADMIN_IDS or [])
    return int(user_id) in admin_ids


@dataclass
class DashboardData:
    account_connected: bool
    source: Optional[Dict[str, Any]]
    targets: List[Dict[str, Any]]
    running: bool
    mode: str


def kb_main(data: DashboardData) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    # Account button depends on connection status
    if data.account_connected:
        rows.append([InlineKeyboardButton(text="🚪 Logout Telegram Account", callback_data="act:logout")])
    else:
        rows.append([InlineKeyboardButton(text="➕ Add Telegram Account", callback_data="act:add_account")])

    rows += [
        [InlineKeyboardButton(text="🎛 Mode", callback_data="act:mode")],
        [InlineKeyboardButton(text="🆔 Scan ID Bot", url="https://t.me/ScanIDBot")],
        [InlineKeyboardButton(text="📥 Set Main Group", callback_data="act:set_source")],
        [InlineKeyboardButton(text="🎯 Targeting Groups", callback_data="act:manage_targets")],
    ]
    if data.running:
        rows.append([InlineKeyboardButton(text="⏹ Stop", callback_data="act:stop")])
    else:
        start_label = "▶ Start Deleting" if data.mode == "delete" else "▶ Start Forwarding"
        rows.append([InlineKeyboardButton(text=start_label, callback_data="act:start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_manage_targets() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Add Group", callback_data="tg:add")],
            [InlineKeyboardButton(text="📋 List Groups", callback_data="tg:list")],
            [InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")],
        ]
    )


def kb_targets_list(targets: List[Dict[str, Any]], page: int = 0, page_size: int = 10) -> InlineKeyboardMarkup:
    """Paginated targets list."""
    total = len(targets)
    if page < 0:
        page = 0

    start = page * page_size
    end = start + page_size
    page_items = targets[start:end]

    rows: List[List[InlineKeyboardButton]] = []
    for t in page_items:
        enabled = bool(t.get("enabled"))
        status = "🟢 ON" if enabled else "🔴 OFF"
        gid = t.get("group_id")
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{status} — {t.get('title','(no title)')}",
                    callback_data=f"tg:details:{gid}:{page}",
                )
            ]
        )

    # Pagination row
    nav: List[InlineKeyboardButton] = []
    if total > page_size:
        max_page = (total - 1) // page_size
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅", callback_data=f"tg:list:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{max_page + 1}", callback_data="noop"))
        if page < max_page:
            nav.append(InlineKeyboardButton(text="➡", callback_data=f"tg:list:{page + 1}"))
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="⬅ Back", callback_data="tg:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def render_dashboard(data: DashboardData) -> str:
    src = "✅" if data.source else "❌"
    account = "✅ Connected" if data.account_connected else "❌ Not Connected"
    running = "🟢 Running" if data.running else "🔴 Stopped"
    mode = "🟦 Forward" if data.mode == "forward" else "🟥 Delete"
    enabled_targets = sum(1 for t in data.targets if t.get("enabled"))
    total_targets = len(data.targets)

    lines = [
        "<b>Forward Bot Dashboard</b>",
        "",
        f"<b>Account:</b> {account}",
        f"<b>Source Group:</b> {src}",
    ]
    if data.source:
        lines.append(
            f"• {data.source.get('title','(no title)')}  (<code>{data.source.get('group_id')}</code>)"
        )

    lines += [
        "",
        f"<b>Mode:</b> {mode}",
        f"<b>Target Groups:</b> {enabled_targets} enabled / {total_targets} total",
        f"<b>State:</b> {running}",
        "",
        "<i>Use the buttons below. No other commands are supported.</i>",
    ]
    return "\n".join(lines)


# =========================
# Forwarding engine
# =========================


class ForwardingEngine:
    def __init__(self, store: MongoStore, shared: SharedTelethon) -> None:
        self.store = store
        self.shared = shared
        self._task: Optional[asyncio.Task] = None
        self._live_handler = None
        self._stop_event = asyncio.Event()

    async def delete_all_from_targets(
        self,
        progress_cb=None,
    ) -> Tuple[bool, str]:
        """Delete all messages in enabled target groups (Targeting Groups).

        NOTE: This is destructive. Requires the connected account to be admin in each target.
        """
        ok, err = await self._permission_precheck()
        if not ok:
            return False, err

        mode = await self.store.get_mode()
        if mode != "delete":
            return False, "Mode is not set to Delete."

        client = await self.shared.get_client()
        enabled_targets = [t for t in await self.store.list_targets() if t.get("enabled")]
        if not enabled_targets:
            return False, "❌ No enabled forward groups to delete."

        # Mark running to prevent parallel starts
        await self.store.set_running(True)

        total_deleted = 0
        BATCH_SIZE = 100

        async def count_messages(entity) -> int:
            # Quick-ish count (still iterates). Good enough for progress.
            c = 0
            async for _ in client.iter_messages(entity):
                c += 1
            return c

        try:
            # Pre-compute total count for progress display
            total = 0
            resolved: List[Tuple[Dict[str, Any], Any, int]] = []
            for t in enabled_targets:
                ent = await self.shared.resolve_chat(str(t["group_id"]))
                cnt = await count_messages(ent)
                resolved.append((t, ent, cnt))
                total += cnt

            done = 0
            if progress_cb:
                await progress_cb(done, total, "deleting")

            for t, ent, _cnt in resolved:
                batch: List[int] = []
                async for msg in client.iter_messages(ent, reverse=True):
                    if self._stop_event.is_set():
                        break
                    if not getattr(msg, "id", None):
                        continue
                    batch.append(int(msg.id))
                    if len(batch) >= BATCH_SIZE:
                        while True:
                            try:
                                await client.delete_messages(ent, batch, revoke=True)
                                total_deleted += len(batch)
                                done += len(batch)
                                batch = []
                                break
                            except FloodWaitError as e:
                                await asyncio.sleep(int(e.seconds) + 1)
                        if progress_cb:
                            await progress_cb(done, total, "deleting")
                        await asyncio.sleep(0.2)

                if batch and not self._stop_event.is_set():
                    while True:
                        try:
                            await client.delete_messages(ent, batch, revoke=True)
                            total_deleted += len(batch)
                            done += len(batch)
                            batch = []
                            break
                        except FloodWaitError as e:
                            await asyncio.sleep(int(e.seconds) + 1)
                    if progress_cb:
                        await progress_cb(done, total, "deleting")

                if self._stop_event.is_set():
                    break

        except Exception as e:  # noqa: BLE001
            logger.exception("Delete-all (targets) failed: %s", e)
            return False, f"❌ Delete failed: {type(e).__name__}"
        finally:
            await self.store.set_running(False)

        return True, f"✅ Deleted {total_deleted} messages from forward groups."

    def is_running_locally(self) -> bool:
        return self._task is not None and not self._task.done()

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

        client = await self.shared.get_client()
        if self._live_handler is not None:
            try:
                client.remove_event_handler(self._live_handler)
            except Exception:  # noqa: BLE001
                pass
        self._live_handler = None
        self._stop_event = asyncio.Event()

    async def _safe_copy(self, client: TelegramClient, to_peer, msg_or_msgs) -> None:
        """Copy messages to target (no 'Forwarded from' attribution)."""
        while True:
            try:
                # Album (list of messages)
                if isinstance(msg_or_msgs, list):
                    medias = [m.media for m in msg_or_msgs if getattr(m, "media", None)]
                    caption = None
                    for m in msg_or_msgs:
                        if getattr(m, "message", None):
                            caption = m.message
                            break
                    if medias:
                        await client.send_file(to_peer, medias, caption=caption)
                    else:
                        if caption:
                            await client.send_message(to_peer, caption)
                    return

                msg = msg_or_msgs
                # Media message
                if getattr(msg, "media", None):
                    await client.send_file(to_peer, msg.media, caption=getattr(msg, "message", None))
                else:
                    text = getattr(msg, "message", None) or ""
                    if text:
                        await client.send_message(to_peer, text)
                return

            except FloodWaitError as e:
                await asyncio.sleep(int(e.seconds) + 1)

    async def _permission_precheck(self) -> Tuple[bool, str]:
        account = await self.store.get_account()
        if not account.get("connected"):
            return False, "❌ Telegram account is not connected. Please add account first."

        source = await self.store.get_source()
        if not source:
            return False, "❌ Source group is not set."

        enabled_targets = [t for t in await self.store.list_targets() if t.get("enabled")]
        if not enabled_targets:
            return False, "❌ No enabled forward groups."

        missing: List[str] = []

        try:
            src_entity = await self.shared.resolve_chat(str(source["group_id"]))
            ok, _ = await self.shared.ensure_admin(src_entity)
            if not ok:
                missing.append(
                    f"• Source: {source.get('title','(no title)')} (<code>{source.get('group_id')}</code>)"
                )
        except Exception as e:  # noqa: BLE001
            missing.append(
                f"• Source: {source.get('title','(no title)')} (<code>{source.get('group_id')}</code>) — {type(e).__name__}"
            )

        for t in enabled_targets:
            try:
                ent = await self.shared.resolve_chat(str(t["group_id"]))
                ok, _ = await self.shared.ensure_admin(ent)
                if not ok:
                    missing.append(
                        f"• Target: {t.get('title','(no title)')} (<code>{t.get('group_id')}</code>)"
                    )
            except Exception:
                missing.append(
                    f"• Target: {t.get('title','(no title)')} (<code>{t.get('group_id')}</code>) — invalid/unreachable id"
                )

        if missing:
            return False, "❌ Missing admin rights in:\n" + "\n".join(missing)

        return True, ""

    async def start(self, progress_cb=None) -> Tuple[bool, str]:
        if self.is_running_locally():
            return False, "⚠ Forwarding is already running."

        ok, err = await self._permission_precheck()
        if not ok:
            return False, err

        mode = await self.store.get_mode()
        if mode != "forward":
            return False, "Mode is not set to Forward."

        await self.store.set_running(True)
        self._task = asyncio.create_task(self._run(progress_cb=progress_cb))
        return True, "✅ Forwarding started."

    async def _run(self, progress_cb=None) -> None:
        client = await self.shared.get_client()
        source = await self.store.get_source()
        if not source:
            await self.store.set_running(False)
            return

        src_entity = await self.shared.resolve_chat(str(source["group_id"]))

        async def iter_enabled_targets():
            return [t for t in await self.store.list_targets() if t.get("enabled")]

        async def forward_one(msg_or_msgs) -> None:
            targets = await iter_enabled_targets()
            if not targets:
                return

            async def send_to_target(tdoc: Dict[str, Any]) -> None:
                if self._stop_event.is_set():
                    return
                try:
                    ent = await self.shared.resolve_chat(str(tdoc["group_id"]))
                    await self._safe_copy(client, ent, msg_or_msgs)
                    # small pacing; real limiting is FloodWait
                    await asyncio.sleep(0.1)
                except UserBannedInChannelError:
                    raise
                except Exception as e:  # noqa: BLE001
                    logger.warning("Forward failed to %s: %s", tdoc.get("group_id"), e)

            # Concurrent copy to many targets (bounded for stability)
            # Adjust this if you want more/less parallelism.
            sem = asyncio.Semaphore(25)

            async def bounded_send(tdoc: Dict[str, Any]) -> None:
                async with sem:
                    await send_to_target(tdoc)

            await asyncio.gather(*(bounded_send(t) for t in targets))

        # History sync: oldest -> newest, grouping albums via grouped_id
        last_gid = None
        album: List[Any] = []

        # Progress (history sync only)
        done = 0
        total = 0
        try:
            # Count total messages for progress (iterates once)
            async for _ in client.iter_messages(src_entity):
                total += 1
            if progress_cb:
                await progress_cb(0, total, "forwarding")

            last_update = 0.0
            async for msg in client.iter_messages(src_entity, reverse=True):
                if self._stop_event.is_set():
                    break

                gid = getattr(msg, "grouped_id", None)

                # Throttle dashboard edits (about 2 per second max)
                now = asyncio.get_running_loop().time()
                if progress_cb and now - last_update >= 0.5:
                    await progress_cb(done, total, "forwarding")
                    last_update = now
                if gid:
                    if last_gid is None:
                        last_gid = gid
                    if gid != last_gid:
                        if album:
                            await forward_one(album)
                        album = [msg]
                        last_gid = gid
                    else:
                        album.append(msg)
                    continue

                if album:
                    await forward_one(album)
                    album = []
                    last_gid = None

                await forward_one(msg)
                done += 1

            if album and not self._stop_event.is_set():
                await forward_one(album)
                # album counts as its message count
                done += len(album)

            if progress_cb:
                await progress_cb(done, total, "forwarding")

            # Auto-stop after completing history sync (no live forwarding).
            return

        except Exception as e:  # noqa: BLE001
            logger.exception("Forwarding engine crashed: %s", e)
        finally:
            await self.store.set_running(False)


# =========================
# Bot application
# =========================


async def build_dashboard_data(store: MongoStore, shared: SharedTelethon) -> DashboardData:
    account = await store.get_account()
    connected = bool(account.get("connected"))
    try:
        if connected:
            connected = await shared.is_authorized()
    except Exception:
        connected = False
    src = await store.get_source()
    targets = await store.list_targets()
    running = await store.get_running()
    mode = await store.get_mode()
    return DashboardData(
        account_connected=connected,
        source=src,
        targets=targets,
        running=running,
        mode=mode,
    )


async def edit_dashboard(
    bot: Bot,
    store: MongoStore,
    admin_id: int,
    text: str,
    markup: InlineKeyboardMarkup,
) -> None:
    info = await store.get_dashboard_message(admin_id)
    if not info:
        return
    try:
        await bot.edit_message_text(
            chat_id=info["chat_id"],
            message_id=info["message_id"],
            text=text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except TelegramBadRequest as e:
        # Happens when user clicks a button but UI state didn't change.
        if "message is not modified" in str(e):
            return
        raise


async def show_home(bot: Bot, store: MongoStore, shared: SharedTelethon, admin_id: int) -> None:
    data = await build_dashboard_data(store, shared)
    await edit_dashboard(bot, store, admin_id, render_dashboard(data), kb_main(data))


def _expect_phone(text: str) -> Optional[str]:
    t = text.strip().replace(" ", "")
    if t.startswith("+") and re.fullmatch(r"\+\d{7,15}", t):
        return t
    if re.fullmatch(r"\d{7,15}", t):
        return "+" + t
    return None


def _expect_otp(text: str) -> Optional[str]:
    t = text.strip().replace(" ", "")
    if re.fullmatch(r"\d{4,10}", t):
        return t
    return None


async def _validate_group_admin(shared: SharedTelethon, ref: str) -> Tuple[bool, str, Optional[int], Optional[str]]:
    """Returns (ok, message, group_id, title)."""
    try:
        entity = await shared.resolve_chat(ref)
    except ValueError as e:
        # Usually means wrong ID format or entity not cached/accessible.
        return (
            False,
            "❌ Cannot resolve this ID. Use:\n"
            "• Basic group: negative id like <code>-123456789</code>\n"
            "• Supergroup/channel: <code>-1001234567890</code>\n"
            f"Details: {str(e)}",
            None,
            None,
        )
    except Exception as e:  # noqa: BLE001
        return False, f"❌ Invalid group ID: {type(e).__name__}", None, None

    gid = getattr(entity, "id", None)
    title = getattr(entity, "title", None) or getattr(entity, "username", None) or "(no title)"

    ok, err = await shared.ensure_admin(entity)
    if not ok:
        return False, err, int(gid), str(title)

    return True, "✅ OK", int(gid), str(title)


async def main() -> None:
    if not config.BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty in config.py")
    if not config.API_ID or not config.API_HASH:
        raise RuntimeError("API_ID / API_HASH not set in config.py")

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    store = MongoStore()
    try:
        await store.ensure_defaults()
    except ServerSelectionTimeoutError as e:
        raise RuntimeError(
            "MongoDB connection error. Please start MongoDB and verify MONGO_URI/MONGO_DB_NAME in config.py"
        ) from e

    shared = SharedTelethon(store)
    engine = ForwardingEngine(store, shared)

    @dp.message(Command("start"))
    async def cmd_start(message: Message) -> None:
        uid = message.from_user.id if message.from_user else 0
        if not is_admin(uid):
            await message.answer(
                f"⛔ Access Denied\n\nYour Telegram user id is: <code>{uid}</code>\nAdd it to ADMIN_IDS in config.py"
            )
            return

        data = await build_dashboard_data(store, shared)
        sent = await message.answer(
            render_dashboard(data),
            reply_markup=kb_main(data),
            disable_web_page_preview=True,
        )
        await store.set_dashboard_message(uid, sent.chat.id, sent.message_id)
        await store.set_ui_state(uid, None)

    # Navigation
    @dp.callback_query(F.data == "nav:home")
    async def cb_home(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return
        await store.set_ui_state(uid, None)
        await show_home(bot, store, shared, uid)
        await callback.answer()

    # Mode selection
    @dp.callback_query(F.data == "act:mode")
    async def cb_mode(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return

        cur = await store.get_mode()
        cur_label = "Forward" if cur == "forward" else "Delete"
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🟦 Forward", callback_data="mode:set:forward")],
                [InlineKeyboardButton(text="🟥 Delete", callback_data="mode:set:delete")],
                [InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")],
            ]
        )
        await edit_dashboard(
            bot,
            store,
            uid,
            "<b>Mode</b>\n\n"
            f"Current: <b>{cur_label}</b>\n\n"
            "🟦 <b>Forward</b>: copies all messages from the source group to enabled forwarding groups.\n"
            "🟥 <b>Delete</b>: deletes all messages from enabled forwarding groups.",
            kb,
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("mode:set:"))
    async def cb_mode_set(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return

        mode = callback.data.split(":")[-1]
        await store.set_mode(mode)
        await store.set_ui_state(uid, None)
        await show_home(bot, store, shared, uid)
        await callback.answer("Mode updated")

    # Manage targets menu
    @dp.callback_query(F.data == "act:manage_targets")
    async def cb_manage_targets(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return
        await store.set_ui_state(uid, {"view": "targets_menu"})
        await edit_dashboard(
            bot,
            store,
            uid,
            "<b>Targeting Groups</b>\n\nChoose an option:",
            kb_manage_targets(),
        )
        await callback.answer()

    @dp.callback_query(F.data == "tg:menu")
    async def cb_targets_menu(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return
        await store.set_ui_state(uid, {"view": "targets_menu"})
        await edit_dashboard(
            bot,
            store,
            uid,
            "<b>Targeting Groups</b>\n\nChoose an option:",
            kb_manage_targets(),
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("tg:list"))
    async def cb_targets_list(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return
        # Parse page: tg:list or tg:list:<page>
        parts = (callback.data or "").split(":")
        page = 0
        if len(parts) >= 3:
            try:
                page = int(parts[2])
            except Exception:
                page = 0

        targets = await store.list_targets()
        await store.set_ui_state(uid, {"view": "targets_list", "page": page})
        text = (
            "<b>Target Groups</b>\n\nTap a group to view details."
            if targets
            else "<b>Target Groups</b>\n\nNo groups added yet."
        )
        await edit_dashboard(bot, store, uid, text, kb_targets_list(targets, page=page))
        await callback.answer()

    def kb_group_details(group_id: int, page: int, enabled: bool) -> InlineKeyboardMarkup:
        toggle_text = "🔴 Turn OFF" if enabled else "🟢 Turn ON"
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=toggle_text, callback_data=f"tg:toggle:{group_id}:{page}")],
                [InlineKeyboardButton(text="🗑 Remove Group", callback_data=f"tg:remove:{group_id}:{page}")],
                [InlineKeyboardButton(text="⬅ Back", callback_data=f"tg:list:{page}")],
            ]
        )

    @dp.callback_query(F.data.startswith("tg:details:"))
    async def cb_group_details(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return

        parts = (callback.data or "").split(":")
        # tg:details:<gid>:<page>
        try:
            gid = int(parts[2])
        except Exception:
            await callback.answer("Invalid", show_alert=True)
            return
        page = 0
        if len(parts) >= 4:
            try:
                page = int(parts[3])
            except Exception:
                page = 0

        doc = await store.get_target(gid)
        if not doc:
            await callback.answer("Not found", show_alert=True)
            return

        title = doc.get("title", "(no title)")
        enabled = bool(doc.get("enabled"))
        status = "🟢 ON" if enabled else "🔴 OFF"
        text = (
            "<b>Group Details</b>\n\n"
            f"<b>Name:</b> {title}\n"
            f"<b>ID:</b> <code>{gid}</code>\n"
            f"<b>Status:</b> {status}"
        )
        await edit_dashboard(bot, store, uid, text, kb_group_details(gid, page, enabled))
        await callback.answer()

    @dp.callback_query(F.data == "noop")
    async def cb_noop(callback: CallbackQuery) -> None:
        # Used for pagination indicator button
        await callback.answer()

    @dp.callback_query(F.data.startswith("tg:toggle:"))
    async def cb_toggle_target(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return

        parts = (callback.data or "").split(":")
        try:
            gid = int(parts[2])
        except Exception:
            await callback.answer("Invalid", show_alert=True)
            return
        page = 0
        if len(parts) >= 4:
            try:
                page = int(parts[3])
            except Exception:
                page = 0

        doc = await store.get_target(gid)
        if not doc:
            await callback.answer("Not found", show_alert=True)
            return

        enabled = bool(doc.get("enabled"))
        await store.set_target_enabled(gid, not enabled)

        # Re-render details
        updated = await store.get_target(gid)
        if not updated:
            await callback.answer("Not found", show_alert=True)
            return

        title = updated.get("title", "(no title)")
        enabled2 = bool(updated.get("enabled"))
        status = "🟢 ON" if enabled2 else "🔴 OFF"
        text = (
            "<b>Group Details</b>\n\n"
            f"<b>Name:</b> {title}\n"
            f"<b>ID:</b> <code>{gid}</code>\n"
            f"<b>Status:</b> {status}"
        )
        await edit_dashboard(bot, store, uid, text, kb_group_details(gid, page, enabled2))
        await callback.answer("Updated")

    @dp.callback_query(F.data.startswith("tg:remove:"))
    async def cb_remove_target(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return

        parts = (callback.data or "").split(":")
        try:
            gid = int(parts[2])
        except Exception:
            await callback.answer("Invalid", show_alert=True)
            return
        page = 0
        if len(parts) >= 4:
            try:
                page = int(parts[3])
            except Exception:
                page = 0

        await store.remove_target(gid)
        targets = await store.list_targets()
        text = (
            "<b>Target Groups</b>\n\nTap a group to view details."
            if targets
            else "<b>Target Groups</b>\n\nNo groups added yet."
        )
        # Clamp page if we removed the last item of the last page
        max_page = (len(targets) - 1) // 10 if targets else 0
        if page > max_page:
            page = max_page

        await edit_dashboard(bot, store, uid, text, kb_targets_list(targets, page=page))
        await callback.answer("Removed")

    @dp.callback_query(F.data == "tg:add")
    async def cb_add_target(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return
        await store.set_ui_state(uid, {"await": "add_target"})
        await edit_dashboard(
            bot,
            store,
            uid,
            "<b>Add Forward Group</b>\n\nSend numeric Group ID:",
            InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="tg:menu")]]
            ),
        )
        await callback.answer()

    # Source group setup
    @dp.callback_query(F.data == "act:set_source")
    async def cb_set_source(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return
        await store.set_ui_state(uid, {"await": "set_source"})
        await edit_dashboard(
            bot,
            store,
            uid,
            "<b>Set Main Group</b>\n\nSend numeric Group ID:",
            InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
            ),
        )
        await callback.answer()

    # Telethon account login/logout
    @dp.callback_query(F.data == "act:add_account")
    async def cb_add_account(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return

        await store.set_account_login(None)
        await store.set_ui_state(uid, {"await": "phone"})
        await edit_dashboard(
            bot,
            store,
            uid,
            "<b>Telegram Account Login</b>\n\nSend phone number (example: <code>+15551234567</code>):",
            InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
            ),
        )
        await callback.answer()

    @dp.callback_query(F.data == "act:logout")
    async def cb_logout(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return

        # Ask confirm/cancel
        await store.set_ui_state(uid, {"view": "logout_confirm"})
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Confirm Logout", callback_data="logout:confirm")],
                [InlineKeyboardButton(text="❌ Cancel", callback_data="logout:cancel")],
            ]
        )
        await edit_dashboard(
            bot,
            store,
            uid,
            "<b>Logout</b>\n\nThis will log out the Telegram account and reset:\n"
            "• Source group\n• Target groups\n• Forwarding state\n\nContinue?",
            kb,
        )
        await callback.answer()

    @dp.callback_query(F.data == "logout:cancel")
    async def cb_logout_cancel(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return
        await store.set_ui_state(uid, None)
        await show_home(bot, store, shared, uid)
        await callback.answer("Cancelled")

    @dp.callback_query(F.data == "logout:confirm")
    async def cb_logout_confirm(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return

        # Stop forwarding if running
        await store.set_running(False)
        await engine.stop()

        # Reset Mongo state (account + groups)
        await store.logout_and_reset()

        # Telethon client still in memory; disconnect and drop instance so new login uses fresh session.
        await shared.reset_client()

        # Admin will need to /start again to re-register dashboard message.
        await callback.answer("Logged out")
        await edit_dashboard(
            bot,
            store,
            uid,
            "✅ Logged out.\n\nAll groups have been reset. Send /start to open the dashboard again.",
            InlineKeyboardMarkup(inline_keyboard=[]),
        )

    # Forward controls
    @dp.callback_query(F.data == "act:start")
    async def cb_start_forward(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return

        # Progress callback updates the dashboard text periodically.
        # We clear it after completion by calling show_home().
        async def progress_cb(done: int, total: int, phase: str) -> None:
            data = await build_dashboard_data(store, shared)
            phase_label = "Forwarding" if phase == "forwarding" else "Deleting"
            progress_line = f"\n\n<b>{phase_label} Progress:</b> <code>{done}/{total}</code>"
            await edit_dashboard(bot, store, uid, render_dashboard(data) + progress_line, kb_main(data))
            # When finished, show completion alert and clear progress.
            if total > 0 and done >= total:
                await show_home(bot, store, shared, uid)
                try:
                    await bot.answer_callback_query(callback.id, text=f"✅ {phase_label} complete", show_alert=False)
                except Exception:
                    pass

        mode = await store.get_mode()
        if mode == "delete":
            ok, msg = await engine.delete_all_from_targets(progress_cb=progress_cb)
            await show_home(bot, store, shared, uid)
            await callback.answer(msg, show_alert=not ok)
            return

        ok, msg = await engine.start(progress_cb=progress_cb)
        # Clear progress line on the dashboard right after starting.
        await show_home(bot, store, shared, uid)
        await callback.answer(msg, show_alert=not ok)

    @dp.callback_query(F.data == "act:stop")
    async def cb_stop_forward(callback: CallbackQuery) -> None:
        uid = callback.from_user.id
        if not is_admin(uid):
            await callback.answer("Access denied", show_alert=True)
            return
        await store.set_running(False)
        await engine.stop()
        await show_home(bot, store, shared, uid)
        await callback.answer("⏹ Stopped")

    # Text input handler for all flows (inline UX, but Telegram requires text messages for input)
    @dp.message(F.text)
    async def on_text(message: Message) -> None:
        uid = message.from_user.id if message.from_user else 0
        if not is_admin(uid):
            return
        if not message.text or message.text.startswith("/"):
            return

        ui_state = await store.get_ui_state(uid)
        if not ui_state or "await" not in ui_state:
            return

        await_what = ui_state.get("await")
        raw = message.text.strip()

        if await_what == "phone":
            phone = _expect_phone(raw)
            if not phone:
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    "<b>Telegram Account Login</b>\n\n❌ Invalid phone. Send again:",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
                return

            try:
                client = await shared.get_client()
                sent = await client.send_code_request(phone)
                await store.set_account_login({"phone": phone, "phone_code_hash": sent.phone_code_hash})
                await store.set_ui_state(uid, {"await": "otp"})
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    "<b>Telegram Account Login</b>\n\nOTP sent. Send the OTP code:",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
            except PhoneNumberInvalidError:
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    "<b>Telegram Account Login</b>\n\n❌ Invalid phone number.",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
            except FloodWaitError as e:
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    f"<b>Telegram Account Login</b>\n\n⏳ FloodWait: wait {int(e.seconds)}s and try again.",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
            except Exception as e:  # noqa: BLE001
                logger.exception("Phone step failed")
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    f"<b>Telegram Account Login</b>\n\n❌ Error: {type(e).__name__}",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
            return

        if await_what == "otp":
            otp = _expect_otp(raw)
            if not otp:
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    "<b>Telegram Account Login</b>\n\n❌ Invalid OTP format. Send digits only:",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
                return

            acc = await store.get_account()
            login = (acc or {}).get("login") or {}
            phone = login.get("phone")
            phone_code_hash = login.get("phone_code_hash")
            if not phone or not phone_code_hash:
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    "<b>Telegram Account Login</b>\n\n❌ Login state missing. Restart with ➕ Add Telegram Account.",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
                await store.set_ui_state(uid, None)
                return

            client = await shared.get_client()
            try:
                await client.sign_in(phone=phone, code=otp, phone_code_hash=phone_code_hash)
            except SessionPasswordNeededError:
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    "<b>Telegram Account Login</b>\n\n❌ 2FA password is enabled on this account. Disable 2FA or use an account without 2FA.",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
                await store.set_ui_state(uid, None)
                return
            except (PhoneCodeInvalidError, PhoneCodeExpiredError):
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    "<b>Telegram Account Login</b>\n\n❌ OTP invalid/expired. Click ➕ Add Telegram Account again.",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
                await store.set_ui_state(uid, None)
                return
            except FloodWaitError as e:
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    f"<b>Telegram Account Login</b>\n\n⏳ FloodWait: wait {int(e.seconds)}s and try again.",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
                return
            except Exception as e:  # noqa: BLE001
                logger.exception("OTP step failed")
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    f"<b>Telegram Account Login</b>\n\n❌ Error: {type(e).__name__}",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
                await store.set_ui_state(uid, None)
                return

            ss = await shared.export_string_session()
            await store.set_account_connected(True, ss)
            await store.set_ui_state(uid, None)
            await show_home(bot, store, shared, uid)
            return

        # Source group input
        if await_what == "set_source":
            ok, msg, gid, title = await _validate_group_admin(shared, raw)
            if not ok:
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    f"<b>Set Main Group</b>\n\n{msg}",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="nav:home")]]
                    ),
                )
                return

            await store.set_source(gid, title)
            await store.set_ui_state(uid, None)
            await show_home(bot, store, shared, uid)
            return

        # Add target group input
        if await_what == "add_target":
            ok, msg, gid, title = await _validate_group_admin(shared, raw)
            if not ok:
                # Missing admin rights
                await edit_dashboard(
                    bot,
                    store,
                    uid,
                    f"<b>Add Forward Group</b>\n\n{msg}",
                    InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="tg:menu")]]
                    ),
                )
                return

            await store.upsert_target(gid, title, enabled=True)
            await store.set_ui_state(uid, None)
            await edit_dashboard(
                bot,
                store,
                uid,
                f"<b>Add Forward Group</b>\n\n✅ Added: {title}",
                InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="tg:menu")]]
                ),
            )
            return

    logger.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
