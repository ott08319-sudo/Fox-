import os
import re
import time
import uuid
import hmac
import hashlib
import asyncio
import logging
import orjson
import random
import aiohttp
import asyncpg
from decimal import Decimal
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import Message, LabeledPrice
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

logging.basicConfig(level=logging.INFO)

# ========== ENV ==========
BOT_TOKEN       = os.getenv("BOT_TOKEN")
ADMIN_ID        = int(os.getenv("ADMIN_ID", "0") or "0")
PORT            = int(os.getenv("PORT", "10000"))
DATABASE_URL    = os.getenv("DATABASE_URL", "")
SELF_URL        = os.getenv("SELF_URL", "")

BC_API_KEY      = os.getenv("BYTECOIN_API_KEY", "")
BC_SECRET       = os.getenv("BYTECOIN_WEBHOOK_SECRET", "")
BC_ENABLED      = os.getenv("BYTECOIN_ENABLED", "true").lower() == "true"
BC_BASE         = "https://bytecoin.space/api/public/v1"
BC_MIN_TRANSFER = Decimal("0.0000001")

# ========== КОНСТАНТЫ ==========
DEFAULT_MIN_BET  = 100
DEFAULT_MAX_BET  = 100000

CHAT_TARIFFS = {
    "standard": {"name": "Стандарт", "percent": 5,  "emoji": "🥉"},
    "pro":      {"name": "Про",      "percent": 7,  "emoji": "🥈"},
    "vip":      {"name": "VIP",      "percent": 10, "emoji": "🥇"},
}

SLOT_SYMBOLS = ["bar", "grapes", "lemon", "seven"]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_http = None
_pool = None

class AdminState(StatesGroup):
    wait_min_bet = State()
    wait_max_bet = State()
    wait_buy_rate = State()
    wait_sell_rate = State()
    wait_spins_count = State()
    wait_spins_acts = State()
    wait_spins_bet = State()
    wait_broadcast = State()

class WithdrawState(StatesGroup):
    amount = State()

# ========== HTTP / БД ==========
async def http():
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    return _http

def clean_db_url(url):
    if not url: return url
    url = url.replace("postgres://", "postgresql://", 1)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs.pop("sslmode", None)
    qs.pop("channel_binding", None)
    if parsed.hostname and "neon.tech" in parsed.hostname:
        qs["ssl"] = ["true"]
    return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

async def get_pool():
    global _pool
    if _pool is None:
        for attempt in range(5):
            try:
                _pool = await asyncpg.create_pool(clean_db_url(DATABASE_URL), min_size=1, max_size=10)
                logging.info("✅ DB connected")
                break
            except Exception as e:
                logging.error(f"DB retry {attempt+1}/5: {e}")
                await asyncio.sleep(5)
    return _pool

# ========== ХЕЛПЕРЫ ==========
def fmt(value):
    s = f"{Decimal(str(value)):.4f}".rstrip("0").rstrip(".")
    return s if s else "0"

def decode_slots(value: int):
    v = value - 1
    b1 = SLOT_SYMBOLS[v & 3]
    b2 = SLOT_SYMBOLS[(v >> 2) & 3]
    b3 = SLOT_SYMBOLS[(v >> 4) & 3]
    return b1, b2, b3

async def log_error(service, error_type, message):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            recent = await db.fetchrow("SELECT id FROM error_log WHERE service=$1 AND error_type=$2 AND created_at > $3 LIMIT 1", service, error_type, time.time() - 300)
            if recent: return
            await db.execute("INSERT INTO error_log (service, error_type, message, created_at) VALUES ($1,$2,$3,$4)", service, error_type, str(message)[:500], time.time())
    except Exception: pass

async def notify_admin(text):
    try: await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception: pass

# ========== БАЗА ==========
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT, first_name TEXT,
            balance NUMERIC(20,9) DEFAULT 0,
            free_spins INT DEFAULT 0,
            referrer_id BIGINT,
            blocked BOOLEAN DEFAULT FALSE,
            created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS bets (
            id SERIAL PRIMARY KEY,
            user_id BIGINT, game TEXT, bet NUMERIC(20,9),
            payout NUMERIC(20,9), multiplier NUMERIC(10,4),
            chat_id BIGINT, chat_title TEXT, meta JSONB,
            created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS transfers (
            id SERIAL PRIMARY KEY,
            user_id BIGINT, direction TEXT, amount NUMERIC(20,9),
            idem_key TEXT UNIQUE, transaction_id TEXT UNIQUE,
            status TEXT DEFAULT 'pending', raw JSONB,
            created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS webhook_inbox (
            event_id TEXT PRIMARY KEY, event_type TEXT, payload JSONB,
            created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS webhook_jobs (
            id SERIAL PRIMARY KEY, event_id TEXT UNIQUE,
            event_type TEXT, payload JSONB, status TEXT DEFAULT 'pending',
            retry_count INT DEFAULT 0, last_error TEXT,
            processed_at DOUBLE PRECISION, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, value TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS partner_chats (
            chat_id BIGINT PRIMARY KEY, chat_title TEXT, owner_id BIGINT,
            chat_type TEXT DEFAULT 'standard', percent NUMERIC(5,2) DEFAULT 5,
            active BOOLEAN DEFAULT TRUE, created_at DOUBLE PRECISION)""")
        await db.execute("ALTER TABLE partner_chats ADD COLUMN IF NOT EXISTS tariff TEXT DEFAULT 'standard'")
        await db.execute("""CREATE TABLE IF NOT EXISTS free_spin_promos (
            id SERIAL PRIMARY KEY,
            spins_per_user INT, total_activations INT, left_activations INT,
            bet_amount NUMERIC(20,9), active BOOLEAN DEFAULT TRUE, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_claims (
            user_id BIGINT, promo_id INT, claimed_at DOUBLE PRECISION,
            PRIMARY KEY (user_id, promo_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY, service TEXT, error_type TEXT, message TEXT, created_at DOUBLE PRECISION)""")

        defaults = {
            "min_bet": DEFAULT_MIN_BET, "max_bet": DEFAULT_MAX_BET,
            "min_withdraw": 1, "buy_rate": 1000, "sell_rate": 1250,
            "min_sell_stars": 15, "channel_url": "https://t.me/foxcasinobyte",
            "chat_url": "https://t.me/foxcasinochatik", "bonus_football_goal": "1.25",
            "bonus_football_nine": "2.0", "bonus_basket_normal": "1.5",
            "bonus_basket_stuck": "0.5", "bonus_basket_clean": "2.5",
            "bonus_darts_red": "1.3", "bonus_darts_white": "1.6",
            "bonus_darts_center": "2.5", "bonus_bowling_4": "1.5",
            "bonus_bowling_5": "1.88", "bonus_bowling_strike": "2.2",
            "bonus_slots_777": "12", "bonus_slots_bar": "7",
            "bonus_slots_lemon": "4", "bonus_slots_grape": "3",
            "bonus_slots_77": "2", "bonus_slots_barbar": "1.5",
            "bonus_dice_pvp": "1.9"
        }
        for k, v in defaults.items():
            await db.execute("INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO NOTHING", k, str(v))

async def get_setting(key, default=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else default

async def set_setting(key, value):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("INSERT INTO settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2", key, str(value))

async def get_balance(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT balance FROM users WHERE user_id=$1", user_id)
        return Decimal(str(row["balance"])) if row else Decimal(0)

async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def register_user(user_id, username, first_name):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""INSERT INTO users (user_id, username, first_name, created_at)
            VALUES ($1,$2,$3,$4) ON CONFLICT (user_id) DO UPDATE
            SET username=EXCLUDED.username, first_name=EXCLUDED.first_name""",
            user_id, username, first_name, time.time())

async def add_balance(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", float(amount), user_id)

async def get_owned_chat(chat_id, user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow(
            "SELECT * FROM partner_chats WHERE chat_id=$1 AND owner_id=$2",
            chat_id, user_id
        )

# ========== BYTECOIN API ==========
def _bc_headers(idem=None):
    h = {"Content-Type": "application/json"}
    if BC_API_KEY.startswith("Bearer "): h["Authorization"] = BC_API_KEY
    elif BC_API_KEY: h["X-API-Key"] = BC_API_KEY
    if idem: h["Idempotency-Key"] = idem
    return h

async def bc_transfer(user_id, amount):
    if not BC_ENABLED: return {"status": "error", "error": "disabled"}
    amt = Decimal(str(amount)).quantize(Decimal("0.000000001"))
    if amt < BC_MIN_TRANSFER: return {"status": "error", "error": "min transfer"}
    idem = f"wd-{user_id}-{uuid.uuid4()}"
    s = await http()
    for _ in range(3):
        try:
            async with s.post(f"{BC_BASE}/service/transfer", json={"user_id": int(user_id), "sum": f"{amt:.9f}"}, headers=_bc_headers(idem)) as r:
                if r.status == 429: await asyncio.sleep(2); continue
                return orjson.loads(await r.read())
        except Exception: await asyncio.sleep(2)
    return {"status": "error", "error": "network"}

# ========== ВЕБХУК BYTECOIN ==========
def _verify_bc_signature(headers, raw):
    event_id = headers.get("x-bytecoin-event-id", "")
    ts = headers.get("x-bytecoin-timestamp", "")
    sig = headers.get("x-bytecoin-signature", "")
    if not BC_SECRET or not event_id or not ts.isdigit(): return None
    signed = f"{ts}.{event_id}.".encode() + raw
    expected = "v1=" + hmac.new(BC_SECRET.encode(), signed, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected): return None
    return orjson.loads(raw)

async def bc_webhook(request: web.Request):
    raw = await request.read()
    h = {k.lower(): v for k, v in request.headers.items()}
    try:
        event = _verify_bc_signature(h, raw)
        if not event: return web.json_response({"error": "invalid sig"}, status=401)
    except Exception: return web.json_response({"error": "bad"}, status=401)

    pool = await get_pool()
    async with pool.acquire() as db:
        try:
            await db.execute("INSERT INTO webhook_inbox (event_id, event_type, payload, created_at) VALUES ($1,$2,$3,$4) ON CONFLICT (event_id) DO NOTHING", event.get("id"), event.get("event"), orjson.dumps(event).decode('utf-8'), time.time())
            await db.execute("INSERT INTO webhook_jobs (event_id, event_type, payload, created_at) VALUES ($1,$2,$3,$4) ON CONFLICT (event_id) DO NOTHING", event.get("id"), event.get("event"), orjson.dumps(event).decode('utf-8'), time.time())
        except Exception: pass
    return web.Response(status=204)

async def webhook_worker():
    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as db:
                job = await db.fetchrow("SELECT id, event_type, payload FROM webhook_jobs WHERE status='pending' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED")
                if job:
                    try:
                        ev = orjson.loads(job["payload"])
                        if job["event_type"] == "transfer.received":
                            d = ev.get("data", {})
                            if d.get("side") == "to_service":
                                uid, amount, txid = int(d["user_id"]), Decimal(str(d["sum"])), d["transaction_id"]
                                async with db.transaction():
                                    res = await db.execute("INSERT INTO transfers (user_id, direction, amount, transaction_id, status, created_at) VALUES ($1,'in',$2,$3,'ok',$4) ON CONFLICT (transaction_id) DO NOTHING", uid, amount, txid, time.time())
                                    if res == "INSERT 0 1":
                                        await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", float(amount), uid)
                                        try: await bot.send_message(uid, f"✅ Пополнение: +{fmt(amount)} BC")
                                        except: pass
                        await db.execute("UPDATE webhook_jobs SET status='done', processed_at=$1 WHERE id=$2", time.time(), job["id"])
                    except Exception as e:
                        await db.execute("UPDATE webhook_jobs SET status='failed', last_error=$1 WHERE id=$2", str(e), job["id"])
        except Exception: pass
        await asyncio.sleep(2)

# ========== ИГРЫ (ЯДРО) ==========
async def play_game(msg: types.Message, game: str, bet: float, is_freespin=False, meta=None):
    uid = msg.from_user.id
    min_bet = int(await get_setting("min_bet", 100))
    max_bet = int(await get_setting("max_bet", 100000))

    if not is_freespin:
        if bet < min_bet: return await msg.reply(f"⚠️ Мин. ставка: {min_bet} BC")
        if bet > max_bet: return await msg.reply(f"⚠️ Макс. ставка: {max_bet} BC")
        bal = await get_balance(uid)
        if bal < bet: return await msg.reply("❌ Недостаточно BC")
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", bet, uid)

    emojis = {"футбол": "⚽", "баскет": "🏀", "дартс": "🎯", "боулинг": "🎳", "слоты": "🎰"}
    name = msg.from_user.first_name
    await msg.reply(f"{emojis[game]} {name}, твоя ставка <b>{bet} BC</b> будет разыграна прямо сейчас!")

    sent = await bot.send_dice(msg.chat.id, emoji=emojis[game])
    await asyncio.sleep(4)
    val = sent.dice.value

    mult = 0
    text = ""
    if game == "футбол":
        if val == 1: text = "😢 Мимо"
        elif val in (2,3,4): text = "⚽ Гол!"; mult = 1.25
        elif val == 5: text = "9️⃣⚽ Девятка!"; mult = 2.0
    elif game == "баскет":
        if val in (1,2): text = "😢 Мимо"
        elif val in (3,4): text = "🏀 Обычный!"; mult = 1.5
        elif val == 5: text = "🔥🏀 Чистый!"; mult = 2.5
    elif game == "дартс":
        if val in (1,2): text = "😢 Мимо"
        elif val in (3,4): text = "🔴 Красное!"; mult = 1.3
        elif val == 5: text = "⚪ Белое!"; mult = 1.6
        elif val == 6: text = "🔥🎯 Центр!"; mult = 2.5
    elif game == "боулинг":
        if val in (1,2,3): text = "😢 Мимо"
        elif val == 4: text = "4️⃣ 4 кегли"; mult = 1.5
        elif val == 5: text = "5️⃣ 5 кеглей"; mult = 1.88
        elif val == 6: text = "🔥🎳 Страйк!"; mult = 2.2
    elif game == "слоты":
        b1, b2, b3 = decode_slots(val)
        if b1 == b2 == b3:
            if b1 == "seven": mult, text = 12, "7️⃣7️⃣7️⃣"
            elif b1 == "bar": mult, text = 7, "BAR BAR BAR"
            elif b1 == "lemon": mult, text = 4, "🍋🍋🍋"
            else: mult, text = 3, "🍇🍇🍇"
        elif (b1 == b2 == "seven") or (b1 == b3 == "seven") or (b2 == b3 == "seven"):
            mult, text = 2, "две 7"
        elif (b1 == b2 == "bar") or (b1 == b3 == "bar") or (b2 == b3 == "bar"):
            mult, text = 1.5, "два BAR"
        else:
            mult, text = 0, "Пусто"

    payout = bet * mult
    meta_json = orjson.dumps(meta or {}).decode()
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("INSERT INTO bets (user_id, game, bet, payout, multiplier, chat_id, chat_title, meta, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", uid, game, bet, payout, mult, msg.chat.id, msg.chat.title, meta_json, time.time())

        partner = await db.fetchrow("SELECT owner_id, percent FROM partner_chats WHERE chat_id=$1 AND active=TRUE", msg.chat.id)
        if partner:
            partner_cut = bet * (float(partner["percent"]) / 100)
            if partner_cut > 0:
                await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", partner_cut, partner["owner_id"])

    if payout > 0:
        await add_balance(uid, payout)
        await msg.reply(f"{text} (x{mult})\n\n{name} выиграл(а) <b>{fmt(payout)} BC</b>")
    else:
        await msg.reply(f"{text}\n\n{name} проиграл(а) <b>{bet} BC</b>")

# ========== ТЕКСТОВЫЕ КОМАНДЫ В ЧАТЕ ==========
@dp.message(F.text.lower().startswith("стата"))
async def cmd_stats(msg: types.Message):
    u = await get_user(msg.from_user.id)
    if not u: return
    pool = await get_pool()
    async with pool.acquire() as db:
        st = await db.fetchrow(
            "SELECT COALESCE(SUM(CASE WHEN payout > 0 THEN payout ELSE 0 END),0) w, "
            "COALESCE(SUM(CASE WHEN payout=0 AND (meta IS NULL OR NOT (meta ? 'freespin')) THEN bet ELSE 0 END),0) l "
            "FROM bets WHERE user_id=$1", msg.from_user.id)
    await msg.reply(f"👤 <b>Профиль</b> @{u['username']}\n💰 Баланс: <b>{fmt(u['balance'])} BC</b>\n\n📊 Выиграно: {fmt(st['w'])} | Проиграно: {fmt(st['l'])}")

GAME_RE = re.compile(r"^(футбол|баскет|дартс|боулинг|слоты)\s+(\d+)$", re.IGNORECASE)

@dp.message(F.text.func(lambda t: bool(GAME_RE.match(t))))
async def handle_game(msg: types.Message):
    if msg.chat.type not in ("group", "supergroup"): return
    m = GAME_RE.match(msg.text)
    await play_game(msg, m.group(1).lower(), float(m.group(2)))

# ========== ФРИСПИНЫ ==========
@dp.callback_query(F.data.startswith("claim_spins_"))
async def cb_claim_spins(cq: types.CallbackQuery):
    pid = int(cq.data.split("_")[2])
    uid = cq.from_user.id
    pool = await get_pool()
    async with pool.acquire() as db:
        claimed = await db.fetchrow("SELECT 1 FROM promo_claims WHERE user_id=$1 AND promo_id=$2", uid, pid)
        if claimed: return await cq.answer("❌ Ты уже забирал!", show_alert=True)
        promo = await db.fetchrow("SELECT * FROM free_spin_promos WHERE id=$1 AND active=TRUE", pid)
        if not promo or promo["left_activations"] <= 0: return await cq.answer("❌ Закончились!", show_alert=True)

        await db.execute("INSERT INTO promo_claims (user_id, promo_id, claimed_at) VALUES ($1,$2,$3)", uid, pid, time.time())
        await db.execute("UPDATE free_spin_promos SET left_activations = left_activations - 1 WHERE id=$1", pid)
        await db.execute("UPDATE users SET free_spins = free_spins + $1 WHERE user_id=$2", promo["spins_per_user"], uid)
        left = promo["left_activations"] - 1
        text = f"🎁 Осталось {left}/{promo['total_activations']} активаций"
        try: await cq.message.edit_text(cq.message.text + f"\n\n{text}", reply_markup=cq.message.reply_markup)
        except: pass
    await cq.answer(f"✅ Получено {promo['spins_per_user']} фриспинов!")

FREE_RE = re.compile(r"^(футбол|баскет|дартс|боулинг|слоты)\s+фриспин$", re.IGNORECASE)

@dp.message(F.text.func(lambda t: bool(FREE_RE.match(t))))
async def handle_freespin(msg: types.Message):
    if msg.chat.type not in ("group", "supergroup"): return
    game = FREE_RE.match(msg.text).group(1).lower()
    uid = msg.from_user.id
    pool = await get_pool()
    async with pool.acquire() as db:
        u = await db.fetchrow("SELECT free_spins FROM users WHERE user_id=$1", uid)
        if not u or u["free_spins"] <= 0:
            return await msg.reply("❌ Нет фриспинов.")
        promo = await db.fetchrow("SELECT bet_amount FROM free_spin_promos WHERE active=TRUE ORDER BY id DESC LIMIT 1")
        bet = float(promo["bet_amount"]) if promo else 100.0
        await db.execute("UPDATE users SET free_spins = free_spins - 1 WHERE user_id=$1", uid)
    await play_game(msg, game, bet, is_freespin=True, meta={"freespin": True})
    await msg.reply("🎁 (Сыграно за фриспин)")

# ========== МЕНЮ В ЛС ==========
async def main_menu_kb():
    ch_url = await get_setting("channel_url", "https://t.me/foxcasinobyte")
    chat_url = await get_setting("chat_url", "https://t.me/foxcasinochatik")
    kb = InlineKeyboardBuilder()
    kb.button(text="👤 Профиль", callback_data="menu_profile")
    kb.button(text="🎮 Играть", url=chat_url)
    kb.button(text="💵 Пополнить", callback_data="menu_deposit")
    kb.button(text="💸 Вывести", callback_data="menu_withdraw")
    kb.button(text="🛒 Купить / Продать", callback_data="menu_exchange")
    kb.button(text="❓ Помощь", callback_data="menu_help")
    kb.button(text="📢 Канал", url=ch_url)
    kb.button(text="🏆 Топ чатов", callback_data="menu_top")
    kb.adjust(2, 2, 2, 2)
    return kb.as_markup()

@dp.message(CommandStart())
async def cmd_start(msg: types.Message):
    args = msg.text.split()
    payload = args[1] if len(args) > 1 else ""
    await register_user(msg.from_user.id, msg.from_user.username or "", msg.from_user.first_name or "")

    if payload == "buy_bc": await show_buy_bc(msg)
    elif payload == "deposit": await show_deposit(msg)
    else: await msg.answer("🎰 <b>FOX CASINO</b>\n\nДобро пожаловать в личный кабинет!\nВыберите действие:", reply_markup=await main_menu_kb())

@dp.callback_query(F.data == "menu_main")
async def cb_main(cq: types.CallbackQuery):
    await cq.message.edit_text("🎰 <b>FOX CASINO</b>\n\nВыберите действие:", reply_markup=await main_menu_kb())
    await cq.answer()

@dp.callback_query(F.data == "menu_profile")
async def cb_profile(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    if not u: return await cq.answer("Сначала /start", show_alert=True)
    pool = await get_pool()
    async with pool.acquire() as db:
        st = await db.fetchrow(
            "SELECT COALESCE(SUM(CASE WHEN payout > 0 THEN payout ELSE 0 END),0) w, "
            "COALESCE(SUM(CASE WHEN payout=0 AND (meta IS NULL OR NOT (meta ? 'freespin')) THEN bet ELSE 0 END),0) l "
            "FROM bets WHERE user_id=$1", cq.from_user.id)

    text = f"👤 <b>Профиль</b>\n\n🏷 Имя: {u['first_name']}\n🔖 @{u['username']}\n🆔 {u['user_id']}\n\n📊 Выиграно: {fmt(st['w'])} BC\n❌ Проиграно: {fmt(st['l'])} BC\n🎮 Фриспинов: {u['free_spins']} шт."
    kb = InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="menu_main").as_markup()
    await cq.message.edit_text(text, reply_markup=kb)
    await cq.answer()

@dp.callback_query(F.data == "menu_deposit")
async def cb_deposit(cq: types.CallbackQuery):
    await show_deposit(cq.message)
    await cq.answer()

async def show_deposit(msg: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="💳 Перейти в ByteCoin", url="https://t.me/byteappbot/app?startapp=transfer-aab82ed9c46841737f935d0f")
    kb.button(text="◀️ Назад", callback_data="menu_main").adjust(1)
    text = "💵 <b>Пополнение баланса</b>\n\n1️⃣ Нажми кнопку ниже\n2️⃣ Введи сумму\n3️⃣ Отправь перевод\n\n✅ Монеты зачислятся автоматически."
    try: await msg.edit_text(text, reply_markup=kb.as_markup())
    except Exception: await msg.answer(text, reply_markup=kb.as_markup())

async def show_buy_bc(msg: types.Message):
    rate = int(await get_setting("buy_rate", 1000))
    kb = InlineKeyboardBuilder()
    for stars in [1, 5, 10, 25]:
        kb.button(text=f"{stars}⭐ = {stars*rate} BC", callback_data=f"buy_stars_{stars}")
    kb.button(text="◀️ Назад", callback_data="menu_main").adjust(2, 2, 1)
    text = f"🛒 <b>Купить BC за ⭐</b>\n\nКурс: 1⭐ = {rate} BC\n\nВыбери пакет:"
    try: await msg.edit_text(text, reply_markup=kb.as_markup())
    except Exception: await msg.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("buy_stars_"))
async def cb_buy_stars(cq: types.CallbackQuery):
    stars = int(cq.data.split("_")[2])
    rate = int(await get_setting("buy_rate", 1000))
    bc = stars * rate
    await bot.send_invoice(cq.from_user.id, title=f"Покупка {bc} BC", description=f"Курс: 1⭐ = {rate} BC", payload=f"buy_bc_{cq.from_user.id}_{stars}", provider_token="", currency="XTR", prices=[LabeledPrice(label=f"{stars}⭐", amount=stars)])
    await cq.answer()

@dp.pre_checkout_query()
async def pre_checkout(pcq: types.PreCheckoutQuery): await pcq.answer(ok=True)

@dp.message(F.successful_payment)
async def on_payment(msg: types.Message):
    payload = msg.successful_payment.invoice_payload
    try:
        _, _, uid_str, stars_str = payload.split("_")
        uid, stars = int(uid_str), int(stars_str)
    except Exception:
        return await msg.answer("❌ Некорректный платёж.")
    if uid != msg.from_user.id:
        return await msg.answer("❌ Платёж не ваш.")
    rate = int(await get_setting("buy_rate", 1000))
    bc = stars * rate
    await add_balance(uid, bc)
    await msg.answer(f"✅ Оплата прошла!\n⭐ {stars}⭐\n💰 +{bc} BC")

# ========== ВЫВОД ==========
@dp.callback_query(F.data == "menu_withdraw")
async def cb_withdraw(cq: types.CallbackQuery, state: FSMContext):
    bal = await get_balance(cq.from_user.id)
    if bal <= 0:
        await state.clear()
        kb = InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="menu_main").as_markup()
        return await cq.message.edit_text("💸 <b>Вывод</b>\n\n❌ Копилка пуста.", reply_markup=kb)
    kb = InlineKeyboardBuilder()
    kb.button(text=f"💸 Вывести всё ({fmt(bal)} BC)", callback_data="withdraw_all")
    kb.button(text="◀️ Назад", callback_data="menu_main").adjust(1)
    await state.set_state(WithdrawState.amount)
    await cq.message.edit_text(f"💸 <b>Вывод</b>\n\n💰 Баланс: <b>{fmt(bal)} BC</b>\n\nВведи сумму или напиши <b>всё</b>:", reply_markup=kb.as_markup())
    await cq.answer()

@dp.callback_query(F.data == "withdraw_all")
async def cb_withdraw_all(cq: types.CallbackQuery, state: FSMContext):
    await state.clear()
    bal = await get_balance(cq.from_user.id)
    await do_withdraw(cq.message, cq.from_user.id, bal)
    await cq.answer()

@dp.message(WithdrawState.amount)
async def msg_withdraw_amount(msg: types.Message, state: FSMContext):
    txt = msg.text.strip().lower()
    if txt == "/cancel":
        await state.clear()
        return await msg.answer("Отменено.", reply_markup=await main_menu_kb())
    if txt in ("всё", "все"):
        bal = await get_balance(msg.from_user.id)
        await state.clear()
        return await do_withdraw(msg, msg.from_user.id, bal)
    try: amount = Decimal(txt.replace(",", ".")).quantize(Decimal("0.000000001"))
    except: return await msg.answer("❌ Неверная сумма.")
    bal = await get_balance(msg.from_user.id)
    if amount <= 0: await msg.answer("❌ Сумма > 0")
    elif amount > bal: await msg.answer(f"❌ Баланс: {fmt(bal)} BC")
    else:
        await state.clear()
        await do_withdraw(msg, msg.from_user.id, amount)

async def do_withdraw(msg, uid, amount):
    await msg.answer("⏳ Обрабатываю...")
    pool = await get_pool()
    async with pool.acquire() as db:
        res = await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1", float(amount), uid)
        if res == "UPDATE 0":
            return await msg.answer("❌ Недостаточно BC.")

    resp = await bc_transfer(uid, amount)

    if resp.get("status") == "ok":
        txid = resp.get("transaction_id")
        async with pool.acquire() as db:
            await db.execute("INSERT INTO transfers (user_id, direction, amount, transaction_id, status, created_at) VALUES ($1,'out',$2,$3,'ok',$4) ON CONFLICT (transaction_id) DO NOTHING", uid, amount, txid, time.time())
        await msg.answer(f"✅ Выведено: <b>{fmt(amount)} BC</b>\nTX: <code>{str(txid)[:16]}...</code>", reply_markup=InlineKeyboardBuilder().button(text="🏠 Меню", callback_data="menu_main").as_markup())
    else:
        err = str(resp.get("error", ""))
        safe_to_refund = err not in ("network", "timeout")
        if safe_to_refund:
            await add_balance(uid, amount)
            await msg.answer(f"❌ Ошибка: {err}\nСредства возвращены.", reply_markup=InlineKeyboardBuilder().button(text="🏠 Меню", callback_data="menu_main").as_markup())
        else:
            await log_error("withdraw", "unknown_status", f"uid={uid} amount={amount} resp={resp}")
            await notify_admin(f"⚠️ Вывод uid={uid} amount={amount} — статус неизвестен: {err}. Проверь вручную.")
            await msg.answer("⏳ Платёж в обработке. Если не зачислится в течение 10 минут — напиши в поддержку.", reply_markup=InlineKeyboardBuilder().button(text="🏠 Меню", callback_data="menu_main").as_markup())

# ========== ОБМЕН ==========
@dp.callback_query(F.data == "menu_exchange")
async def cb_exchange(cq: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="💵 Купить BC (за ⭐)", callback_data="exchange_buy")
    kb.button(text="💸 Вывести BC", callback_data="menu_withdraw")
    kb.button(text="◀️ Назад", callback_data="menu_main").adjust(1)
    await cq.message.edit_text("🛒 <b>Обмен</b>\n\nВыбери действие:", reply_markup=kb.as_markup())
    await cq.answer()

@dp.callback_query(F.data == "exchange_buy")
async def cb_ex_buy(cq: types.CallbackQuery):
    await show_buy_bc(cq.message)
    await cq.answer()

@dp.callback_query(F.data == "exchange_sell")
async def cb_ex_sell(cq: types.CallbackQuery):
    await cq.answer("❌ Продажа BC за ⭐ временно недоступна.\nВывод через ByteCoin → меню «Вывести».", show_alert=True)

# ========== ТОП И ПОМОЩЬ ==========
@dp.callback_query(F.data == "menu_help")
async def cb_help(cq: types.CallbackQuery):
    text = "❓ <b>Игры</b>\n\n⚽ Футбол (x1.25/x2)\n🏀 Баскет (x1.5/x2.5)\n🎯 Дартс (x1.3/x1.6/x2.5)\n🎳 Боулинг (x1.5/x1.88/x2.2)\n🎰 Слоты (до x12)\n🎲 PvP (x1.9)\n\n📌 Мин: 100 BC | Макс: 100000 BC"
    kb = InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="menu_main").as_markup()
    await cq.message.edit_text(text, reply_markup=kb)
    await cq.answer()

@dp.callback_query(F.data == "menu_top")
async def cb_top(cq: types.CallbackQuery):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT chat_title, SUM(bet) as t FROM bets WHERE chat_id IS NOT NULL GROUP BY chat_id, chat_title ORDER BY t DESC LIMIT 10")
    text = "🏆 <b>Топ чатов:</b>\n\n" + ("\n".join(f"{i}. {r['chat_title']} — {fmt(r['t'])} BC" for i, r in enumerate(rows, 1)) if rows else "Нет данных.")
    kb = InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="menu_main").as_markup()
    await cq.message.edit_text(text, reply_markup=kb)
    await cq.answer()

# ========== РАБОТА С ЧАТАМИ ==========
@dp.my_chat_member()
async def on_chat_member(event: types.ChatMemberUpdated):
    if event.new_chat_member.status == "administrator" and event.from_user.id != bot.id:
        pool = await get_pool()
        async with pool.acquire() as db:
            exists = await db.fetchrow("SELECT 1 FROM partner_chats WHERE chat_id=$1", event.chat.id)
            if not exists:
                await db.execute("INSERT INTO partner_chats (chat_id, chat_title, owner_id, active, percent, tariff, created_at) VALUES ($1,$2,$3,FALSE,5,'standard',$4)", event.chat.id, event.chat.title, event.from_user.id, time.time())
                try: await bot.send_message(event.from_user.id, f"👋 Бот добавлен в чат «{event.chat.title}».\n\nНапиши в чате <b>рег</b>, чтобы зарегистрировать беседу и начать зарабатывать!")
                except: pass

@dp.message(F.text.lower().in_(["рег", "/reg"]))
async def cmd_reg_chat(msg: types.Message):
    if msg.chat.type not in ("group", "supergroup"):
        return await msg.reply("❌ Только в беседах.")

    try:
        member = await bot.get_chat_member(msg.chat.id, msg.from_user.id)
    except Exception:
        return await msg.reply("❌ Не могу проверить права. Убедись, что бот — админ чата.")

    if member.status not in ("administrator", "creator"):
        return await msg.reply("❌ Только админ беседы может её зарегистрировать.")

    uname = f"@{msg.from_user.username}" if msg.from_user.username else msg.from_user.first_name
    pool = await get_pool()
    async with pool.acquire() as db:
        chat = await db.fetchrow("SELECT * FROM partner_chats WHERE chat_id=$1", msg.chat.id)

        if not chat:
            await db.execute("INSERT INTO partner_chats (chat_id, chat_title, owner_id, active, percent, tariff, created_at) VALUES ($1,$2,$3,TRUE,5,'standard',$4)", msg.chat.id, msg.chat.title, msg.from_user.id, time.time())
            return await msg.reply(f"✅ <b>Беседа зарегистрирована!</b>\n\n👑 Владелец: {uname}\n📊 Тариф: 🥉 Стандарт — 5% с игр\n\n🎮 Играть: <code>футбол 100</code>, <code>слоты 500</code> и т.д.\n⚙️ Сменить тариф: /chat")

        if not chat["active"]:
            if chat["owner_id"] != msg.from_user.id:
                try:
                    old = await bot.get_chat_member(msg.chat.id, chat["owner_id"])
                    old_ok = old.status in ("administrator", "creator")
                except Exception:
                    old_ok = False
                if old_ok:
                    return await msg.reply("❌ Чат уже привязан к другому владельцу.\nСмени владельца через /chat или обратись к админу бота.")
            await db.execute("UPDATE partner_chats SET active=TRUE, owner_id=$1, chat_title=$2 WHERE chat_id=$3", msg.from_user.id, msg.chat.title, msg.chat.id)
            return await msg.reply("✅ <b>Беседа активирована!</b>\n\n📊 Тариф: 🥉 Стандарт — 5% с игр\n⚙️ Сменить тариф: /chat")

        if chat["owner_id"] == msg.from_user.id:
            return await msg.reply("✅ Уже зарегистрирована. Сменить тариф: /chat")
        return await msg.reply("❌ Чат уже зарегистрирован другим владельцем.")

@dp.message(Command("chat"))
async def cmd_chat(msg: types.Message):
    if msg.chat.type not in ("group", "supergroup"):
        return await msg.reply("❌ Команда работает только в беседах.")

    chat = await get_owned_chat(msg.chat.id, msg.from_user.id)
    if not chat:
        return await msg.reply("❌ Только владелец зарегистрированного чата может менять тариф.\nСначала напиши <b>рег</b>.")

    current = chat["tariff"] or "standard"
    kb = InlineKeyboardBuilder()
    for key, t in CHAT_TARIFFS.items():
        mark = "✅ " if key == current else ""
        kb.button(text=f"{mark}{t['emoji']} {t['name']} — {t['percent']}%", callback_data=f"chat_tariff_{key}")
    kb.button(text="❌ Закрыть", callback_data="chat_tariff_close").adjust(1)

    text = f"⚙️ <b>Тариф беседы</b>\n\nТекущий: {CHAT_TARIFFS[current]['emoji']} <b>{CHAT_TARIFFS[current]['name']}</b> ({CHAT_TARIFFS[current]['percent']}% с игр)\n\nВыбери новый тариф:"
    await msg.reply(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("chat_tariff_"))
async def cb_chat_tariff(cq: types.CallbackQuery):
    key = cq.data.replace("chat_tariff_", "")

    if key == "close":
        try: await cq.message.delete()
        except: pass
        return await cq.answer()

    if key not in CHAT_TARIFFS:
        return await cq.answer("❌ Неизвестный тариф", show_alert=True)

    chat = await get_owned_chat(cq.message.chat.id, cq.from_user.id)
    if not chat:
        return await cq.answer("❌ Ты не владелец этого чата", show_alert=True)

    t = CHAT_TARIFFS[key]
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE partner_chats SET tariff=$1, percent=$2 WHERE chat_id=$3", key, t["percent"], cq.message.chat.id)

    try:
        await cq.message.edit_text(f"✅ Тариф обновлён: {t['emoji']} <b>{t['name']}</b> — {t['percent']}% с игр.")
    except: pass
    await cq.answer(f"Тариф: {t['name']} ({t['percent']}%)")

# ========== АДМИНКА ==========
def adm_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="⚙️ Настройки", callback_data="adm_settings")
    kb.button(text="🎁 Фриспины", callback_data="adm_spins")
    kb.button(text="📈 Коэффициенты", callback_data="adm_coeffs")
    kb.button(text="🤝 Партнёры", callback_data="adm_partners")
    kb.button(text="✉️ Рассылка", callback_data="adm_broadcast")
    kb.adjust(2, 2, 2)
    return kb.as_markup()

@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=adm_kb())

@dp.callback_query(F.data == "adm_back")
async def cb_adm_back(cq: types.CallbackQuery):
    await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=adm_kb())
    await cq.answer()

@dp.callback_query(F.data == "adm_stats")
async def cb_adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        u = (await db.fetchrow("SELECT COUNT(*) c FROM users"))["c"]
        b = (await db.fetchrow("SELECT COALESCE(SUM(balance),0) s FROM users"))["s"]
        bt = (await db.fetchrow("SELECT COUNT(*) c, COALESCE(SUM(bet),0) s FROM bets"))
    kb = InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="adm_back").as_markup()
    await cq.message.edit_text(f"📊 <b>Статистика</b>\n\n👥 Юзеров: {u}\n💰 Балансы: {fmt(b)} BC\n🎲 Ставок: {bt['c']} (оборот: {fmt(bt['s'])} BC)", reply_markup=kb)
    await cq.answer()

@dp.callback_query(F.data == "adm_settings")
async def cb_adm_settings(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    mb = await get_setting("min_bet", 100)
    xb = await get_setting("max_bet", 100000)
    br = await get_setting("buy_rate", 1000)
    sr = await get_setting("sell_rate", 1250)
    kb = InlineKeyboardBuilder()
    kb.button(text=f"Мин. ставка ({mb})", callback_data="adm_set_min")
    kb.button(text=f"Макс. ставка ({xb})", callback_data="adm_set_max")
    kb.button(text=f"Курс покупки ({br})", callback_data="adm_set_br")
    kb.button(text=f"Курс продажи ({sr})", callback_data="adm_set_sr")
    kb.button(text="◀️ Назад", callback_data="adm_back").adjust(1)
    await cq.message.edit_text("⚙️ <b>Настройки</b>", reply_markup=kb.as_markup())
    await cq.answer()

@dp.callback_query(F.data.startswith("adm_set_"))
async def cb_adm_set_param(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID: return
    param = cq.data.replace("adm_set_", "")
    if param == "min":
        await state.set_state(AdminState.wait_min_bet); text = "Введи новую мин. ставку:"
    elif param == "max":
        await state.set_state(AdminState.wait_max_bet); text = "Введи новую макс. ставку:"
    elif param == "br":
        await state.set_state(AdminState.wait_buy_rate); text = "Введи новый курс покупки (BC за 1⭐):"
    elif param == "sr":
        await state.set_state(AdminState.wait_sell_rate); text = "Введи новый курс продажи (BC за 1⭐):"
    else:
        return await cq.answer("❌ Неизвестный параметр", show_alert=True)
    await cq.message.edit_text(text)
    await cq.answer()

@dp.message(AdminState.wait_min_bet)
async def adm_set_min(msg: types.Message, state: FSMContext):
    try: val = int(msg.text)
    except: return await msg.answer("❌ Нужно целое число.")
    await set_setting("min_bet", val)
    await state.clear()
    await msg.answer("✅ Мин. ставка обновлена.", reply_markup=adm_kb())

@dp.message(AdminState.wait_max_bet)
async def adm_set_max(msg: types.Message, state: FSMContext):
    try: val = int(msg.text)
    except: return await msg.answer("❌ Нужно целое число.")
    await set_setting("max_bet", val)
    await state.clear()
    await msg.answer("✅ Макс. ставка обновлена.", reply_markup=adm_kb())

@dp.message(AdminState.wait_buy_rate)
async def adm_set_br(msg: types.Message, state: FSMContext):
    try: val = int(msg.text)
    except: return await msg.answer("❌ Нужно целое число.")
    await set_setting("buy_rate", val)
    await state.clear()
    await msg.answer("✅ Курс покупки обновлен.", reply_markup=adm_kb())

@dp.message(AdminState.wait_sell_rate)
async def adm_set_sr(msg: types.Message, state: FSMContext):
    try: val = int(msg.text)
    except: return await msg.answer("❌ Нужно целое число.")
    await set_setting("sell_rate", val)
    await state.clear()
    await msg.answer("✅ Курс продажи обновлен.", reply_markup=adm_kb())

@dp.callback_query(F.data == "adm_spins")
async def cb_adm_spins(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID: return
    await state.set_state(AdminState.wait_spins_count)
    await cq.message.edit_text("Введи <b>кол-во фриспинов</b>:")
    await cq.answer()

@dp.message(AdminState.wait_spins_count)
async def adm_spins_count(msg: types.Message, state: FSMContext):
    try: val = int(msg.text)
    except: return await msg.answer("❌ Нужно целое число.")
    await state.update_data(spins=val)
    await state.set_state(AdminState.wait_spins_acts)
    await msg.answer("Введи <b>кол-во активаций</b> (на сколько юзеров):")

@dp.message(AdminState.wait_spins_acts)
async def adm_spins_acts(msg: types.Message, state: FSMContext):
    try: val = int(msg.text)
    except: return await msg.answer("❌ Нужно целое число.")
    await state.update_data(acts=val)
    await state.set_state(AdminState.wait_spins_bet)
    await msg.answer("Введи <b>ставку за спин</b> (в BC):")

@dp.message(AdminState.wait_spins_bet)
async def adm_spins_bet(msg: types.Message, state: FSMContext):
    try: bet = float(msg.text)
    except: return await msg.answer("❌ Нужно число.")
    data = await state.get_data()
    await state.clear()
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("INSERT INTO free_spin_promos (spins_per_user, total_activations, left_activations, bet_amount, active, created_at) VALUES ($1,$2,$2,$3,TRUE,$4) RETURNING id", data["spins"], data["acts"], bet, time.time())
        pid = row["id"]
        chats = await db.fetch("SELECT chat_id FROM partner_chats WHERE active=TRUE")

    kb = InlineKeyboardBuilder().button(text="🎁 Забрать фриспины", callback_data=f"claim_spins_{pid}")
    text = f"🎉 <b>Успейте получить {data['spins']} FREE SPINS!</b>\n🎁 Осталось {data['acts']}/{data['acts']} активаций\nСтавка: {fmt(bet)} BC"

    sent = 0
    for c in chats:
        try:
            await bot.send_message(c["chat_id"], text, reply_markup=kb.as_markup())
            sent += 1
        except: pass
    await msg.answer(f"✅ Фриспины созданы и отправлены в {sent} чатов!", reply_markup=adm_kb())

@dp.callback_query(F.data == "adm_partners")
async def cb_adm_partners(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT * FROM partner_chats ORDER BY created_at DESC LIMIT 20")
    text = "🤝 <b>Чаты:</b>\n\n"
    for r in rows:
        stat = "✅" if r["active"] else "⏳"
        text += f"{stat} {r['chat_title']} (владелец: {r['owner_id']}) — {r['percent']}%\n"
    kb = InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="adm_back").as_markup()
    await cq.message.edit_text(text, reply_markup=kb)
    await cq.answer()

@dp.callback_query(F.data == "adm_broadcast")
async def cb_adm_broadcast(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID: return
    await state.set_state(AdminState.wait_broadcast)
    await cq.message.edit_text("Введи текст для рассылки (или /cancel):")
    await cq.answer()

@dp.message(AdminState.wait_broadcast)
async def adm_broadcast_send(msg: types.Message, state: FSMContext):
    if msg.text.strip().lower() in ("/cancel", "отмена"):
        await state.clear()
        return await msg.answer("Отменено.", reply_markup=adm_kb())
    await state.clear()
    pool = await get_pool()
    async with pool.acquire() as db:
        users = await db.fetch("SELECT user_id FROM users")
    ok, fail = 0, 0
    for u in users:
        try:
            await bot.send_message(u["user_id"], msg.text)
            ok += 1
        except: fail += 1
        await asyncio.sleep(0.05)
    await msg.answer(f"✅ Отправлено: {ok}\n❌ Ошибок: {fail}", reply_markup=adm_kb())

# ========== ЗАПУСК ==========
async def health(_): return web.Response(text="ok")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_post("/webhooks/bytecoin", bc_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web on :{PORT}")

async def main():
    await init_db()
    await start_web()
    asyncio.create_task(webhook_worker())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
