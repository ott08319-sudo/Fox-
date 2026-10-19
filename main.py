import os
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
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, LabeledPrice, Message
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
HOUSE_EDGE     = Decimal("0.02")
DEFAULT_MIN_BET  = 100
DEFAULT_MAX_BET  = 100000
DEFAULT_MIN_WD   = 1

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_http = None
_pool = None
_states = {}

class WithdrawState(StatesGroup):
    amount = State()

class DepositState(StatesGroup):
    stars_amount = State()

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
        _pool = await asyncpg.create_pool(clean_db_url(DATABASE_URL), min_size=1, max_size=10)
    return _pool

# ========== ХЕЛПЕРЫ ==========
def fmt_money(value):
    s = f"{float(value):.4f}".rstrip("0").rstrip(".")
    return s if s else "0"

def escape_html(text):
    if not text: return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def log_error(service, error_type, message):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            recent = await db.fetchrow("SELECT id FROM error_log WHERE service=$1 AND error_type=$2 AND created_at > $3 LIMIT 1", service, error_type, time.time() - 300)
            if recent: return
            await db.execute("INSERT INTO error_log (service, error_type, message, created_at) VALUES ($1,$2,$3,$4)", service, error_type, str(message)[:500], time.time())
    except Exception as e:
        logging.error(f"log_error failed: {e}")

async def notify_admin(text):
    try:
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception:
        pass

# ========== БД: ИНИЦИАЛИЗАЦИЯ ==========
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
            chat_id BIGINT PRIMARY KEY,
            chat_title TEXT, owner_id BIGINT,
            chat_type TEXT DEFAULT 'standard',
            percent NUMERIC(5,2) DEFAULT 5,
            active BOOLEAN DEFAULT TRUE,
            created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS free_spin_promos (
            id SERIAL PRIMARY KEY,
            spins_per_user INT, total_activations INT, left_activations INT,
            bet_amount NUMERIC(20,9), message_id BIGINT, chat_id BIGINT,
            active BOOLEAN DEFAULT TRUE, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_claims (
            user_id BIGINT, promo_id INT, claimed_at DOUBLE PRECISION,
            PRIMARY KEY (user_id, promo_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY,
            service TEXT, error_type TEXT, message TEXT, created_at DOUBLE PRECISION)""")

        defaults = {
            "min_bet": DEFAULT_MIN_BET,
            "max_bet": DEFAULT_MAX_BET,
            "min_withdraw": DEFAULT_MIN_WD,
            "buy_rate": 1000,
            "sell_rate": 1250,
            "min_sell_stars": 15,
            "chat_bonus_spins": 0,
            "channel_url": "https://t.me/foxcasinobyte",
            "chat_url": "https://t.me/foxcasinochatik",
            "bonus_football_goal": "1.25",
            "bonus_football_nine": "2.0",
            "bonus_basket_normal": "1.5",
            "bonus_basket_stuck": "0.5",
            "bonus_basket_clean": "2.5",
            "bonus_darts_red": "1.3",
            "bonus_darts_white": "1.6",
            "bonus_darts_center": "2.5",
            "bonus_bowling_4": "1.5",
            "bonus_bowling_5": "1.88",
            "bonus_bowling_strike": "2.2",
            "bonus_slots_777": "12",
            "bonus_slots_bar": "7",
            "bonus_slots_lemon": "4",
            "bonus_slots_grape": "3",
            "bonus_slots_77": "2",
            "bonus_slots_barbar": "1.5",
            "bonus_dice_pvp": "1.9",
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

# ========== BYTECOIN API ==========
def _bc_headers(idem=None):
    h = {"Content-Type": "application/json"}
    if BC_API_KEY.startswith("Bearer "):
        h["Authorization"] = BC_API_KEY
    else:
        h["X-API-Key"] = BC_API_KEY
    if idem: h["Idempotency-Key"] = idem
    return h

async def bc_transfer(user_id, amount):
    if not BC_ENABLED:
        return {"status": "error", "error": "disabled"}
    amt = Decimal(str(amount)).quantize(Decimal("0.000000001"))
    if amt < BC_MIN_TRANSFER:
        return {"status": "error", "error": "min transfer"}
    idem = f"wd-{user_id}-{uuid.uuid4()}"
    s = await http()
    for _ in range(3):
        try:
            async with s.post(f"{BC_BASE}/service/transfer", json={"user_id": int(user_id), "sum": f"{amt:.9f}"}, headers=_bc_headers(idem)) as r:
                data = orjson.loads(await r.read())
                if r.status == 429:
                    await asyncio.sleep(2); continue
                return data
        except Exception as e:
            logging.error(f"bc_transfer {_}: {e}")
            await asyncio.sleep(2)
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
    except Exception:
        return web.json_response({"error": "bad"}, status=401)

    pool = await get_pool()
    async with pool.acquire() as db:
        try:
            await db.execute("INSERT INTO webhook_inbox (event_id, event_type, payload, created_at) VALUES ($1,$2,$3,$4)",
                event.get("id"), event.get("event"), orjson.dumps(event), time.time())
            await db.execute("INSERT INTO webhook_jobs (event_id, event_type, payload, created_at) VALUES ($1,$2,$3,$4)",
                event.get("id"), event.get("event"), orjson.dumps(event), time.time())
        except Exception:
            pass # Already exists
    return web.Response(status=204)

async def webhook_worker():
    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as db:
                job = await db.fetchrow("SELECT id, event_type, payload FROM webhook_jobs WHERE status='pending' LIMIT 1 FOR UPDATE SKIP LOCKED")
                if job:
                    try:
                        ev = orjson.loads(job["payload"]) if isinstance(job["payload"], str) else job["payload"]
                        if job["event_type"] == "transfer.received":
                            d = ev.get("data", {})
                            if d.get("side") == "to_service":
                                uid, amount, txid = int(d["user_id"]), Decimal(str(d["sum"])), d["transaction_id"]
                                async with db.transaction():
                                    await db.execute("INSERT INTO transfers (user_id, direction, amount, transaction_id, status, created_at) VALUES ($1,'in',$2,$3,'ok',$4) ON CONFLICT (transaction_id) DO NOTHING", uid, amount, txid, time.time())
                                    await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", float(amount), uid)
                                try: await bot.send_message(uid, f"✅ Пополнение: +{fmt_money(amount)} BC")
                                except: pass
                        await db.execute("UPDATE webhook_jobs SET status='done', processed_at=$1 WHERE id=$2", time.time(), job["id"])
                    except Exception as e:
                        await db.execute("UPDATE webhook_jobs SET status='failed', last_error=$1 WHERE id=$2", str(e), job["id"])
        except Exception:
            pass
        await asyncio.sleep(2)

# ========== ГЛАВНОЕ МЕНЮ ==========
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

async def show_main_menu(msg, edit=False):
    text = "🎰 <b>FOX CASINO</b>\n\nДобро пожаловать в личный кабинет!\nВыберите действие:"
    if edit:
        await msg.edit_text(text, reply_markup=await main_menu_kb())
    else:
        await msg.answer(text, reply_markup=await main_menu_kb())

@dp.message(CommandStart())
async def cmd_start(msg: types.Message):
    args = msg.text.split()
    payload = args[1] if len(args) > 1 else ""
    await register_user(msg.from_user.id, msg.from_user.username or "", msg.from_user.first_name or "")
    
    if payload == "buy_bc":
        await show_buy_bc(msg)
    elif payload == "deposit":
        await show_deposit(msg)
    else:
        await show_main_menu(msg)

# ========== ПРОФИЛЬ ==========
@dp.callback_query(F.data == "menu_profile")
async def cb_profile(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    today = time.time() - 86400
    pool = await get_pool()
    async with pool.acquire() as db:
        stats = await db.fetchrow("""SELECT 
            COALESCE(SUM(CASE WHEN payout > 0 THEN payout ELSE 0 END), 0) as won,
            COALESCE(SUM(CASE WHEN payout = 0 THEN bet ELSE 0 END), 0) as lost,
            COALESCE(MAX(payout), 0) as max_win,
            COALESCE(MAX(multiplier) * 100, 0) as max_risk
            FROM bets WHERE user_id=$1 AND created_at > $2""", cq.from_user.id, today)
    
    text = (
        f"👤 <b>Ваш профиль</b>\n\n"
        f"🏷 Имя: {escape_html(u['first_name'] or '')}\n"
        f"🔖 Username: @{escape_html(u['username'] or '')}\n"
        f"🆔 ID: <code>{u['user_id']}</code>\n\n"
        f"📊 <b>Статистика за сегодня:</b>\n"
        f"✅ Выиграно: <b>{fmt_money(stats['won'])} BC</b>\n"
        f"❌ Проиграно: <b>{fmt_money(stats['lost'])} BC</b>\n"
        f"🏆 Макс. выигрыш: <b>{fmt_money(stats['max_win'])} BC</b>\n"
        f"🎲 Самый рискованный: <b>{fmt_money(stats['max_risk'])}%</b>"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="🎁 Копилка", callback_data="menu_wallet")
    kb.button(text="🎮 FREE SPINS", callback_data="menu_freespins")
    kb.button(text="💎 Пополнить BC", callback_data="menu_deposit")
    kb.button(text="⚪ Купить BC", url=f"https://t.me/{(await bot.get_me()).username}?start=buy_bc")
    kb.button(text="◀️ Назад", callback_data="menu_main")
    kb.adjust(2, 2, 1)
    await cq.message.edit_text(text, reply_markup=kb.as_markup())
    await cq.answer()

@dp.callback_query(F.data == "menu_wallet")
async def cb_wallet(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = float(u["balance"]) if u else 0
    await cq.message.edit_text(f"🎁 <b>Копилка</b>\n\n💰 Баланс: <b>{fmt_money(bal)} BC</b>", reply_markup=InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="menu_profile").as_markup())
    await cq.answer()

@dp.callback_query(F.data == "menu_freespins")
async def cb_freespins(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    fs = u["free_spins"] if u else 0
    await cq.message.edit_text(f"🎮 <b>FREE SPINS</b>\n\nДоступно: <b>{fs} шт.</b>", reply_markup=InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="menu_profile").as_markup())
    await cq.answer()

@dp.callback_query(F.data == "menu_main")
async def cb_main(cq: types.CallbackQuery):
    await show_main_menu(cq.message, edit=True)
    await cq.answer()

# ========== ПОПОЛНЕНИЕ (ИНСТРУКЦИЯ) ==========
async def show_deposit(msg):
    kb = InlineKeyboardBuilder()
    kb.button(text="💳 Перейти в ByteCoin", url="https://t.me/byteappbot")
    kb.button(text="◀️ Назад", callback_data="menu_main")
    kb.adjust(1)
    text = (
        "💵 <b>Пополнение баланса</b>\n\n"
        "1️⃣ Нажми кнопку ниже <b>«Перейти в ByteCoin»</b>\n"
        "2️⃣ Введи сумму\n"
        "3️⃣ Отправь перевод на сервис бота\n\n"
        "✅ <b>Монеты зачислятся автоматически</b> в течение нескольких секунд."
    )
    await msg.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "menu_deposit")
async def cb_deposit(cq: types.CallbackQuery):
    await show_deposit(cq.message)
    await cq.answer()

# ========== ПОКУПКА BC ЗА ЗВЁЗДЫ ==========
async def show_buy_bc(msg):
    rate = int(await get_setting("buy_rate", 1000))
    kb = InlineKeyboardBuilder()
    for stars in [1, 5, 10, 25]:
        kb.button(text=f"{stars}⭐ = {stars*rate} BC", callback_data=f"buy_stars_{stars}")
    kb.button(text="◀️ Назад", callback_data="menu_main")
    kb.adjust(2, 2, 1)
    await msg.answer(f"🛒 <b>Купить BC за ⭐</b>\n\nКурс: 1⭐ = {rate} BC\n\nВыбери пакет:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("buy_stars_"))
async def cb_buy_stars(cq: types.CallbackQuery):
    stars = int(cq.data.split("_")[2])
    rate = int(await get_setting("buy_rate", 1000))
    bc = stars * rate
    await bot.send_invoice(
        chat_id=cq.from_user.id,
        title=f"Покупка {bc} BC",
        description=f"Курс: 1⭐ = {rate} BC",
        payload=f"buy_bc_{cq.from_user.id}_{stars}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=f"{stars}⭐", amount=stars)],
    )
    await cq.answer()

@dp.message(F.successful_payment)
async def on_payment(msg: types.Message):
    payload = msg.successful_payment.invoice_payload
    _, _, uid_str, stars_str = payload.split("_")
    uid, stars = int(uid_str), int(stars_str)
    rate = int(await get_setting("buy_rate", 1000))
    bc = stars * rate
    await add_balance(uid, bc)
    await msg.answer(f"✅ <b>Оплата прошла!</b>\n⭐ Оплачено: {stars}⭐\n💰 Начислено: <b>{bc} BC</b>")

@dp.pre_checkout_query()
async def pre_checkout(pcq: types.PreCheckoutQuery):
    await pcq.answer(ok=True)
# ========== ВЫВОД (BYTECOIN) ==========
@dp.callback_query(F.data == "menu_withdraw")
async def cb_withdraw(cq: types.CallbackQuery, state: FSMContext):
    bal = await get_balance(cq.from_user.id)
    min_wd = Decimal(str(await get_setting("min_withdraw", 1)))
    
    if bal < min_wd:
        await cq.message.edit_text(f"💸 <b>Вывод монет</b>\n\n❌ Копилка пуста — нечего выводить.", reply_markup=InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="menu_main").as_markup())
        await cq.answer()
        return
        
    text = f"💸 <b>Вывод монет</b>\n\n💰 Баланс: <b>{fmt_money(bal)} BC</b>\n\nВведите сумму для вывода или напишите <b>всё</b>.\n\n/cancel — отмена"
    kb = InlineKeyboardBuilder()
    kb.button(text=f"💸 Вывести всё ({fmt_money(bal)} BC)", callback_data="withdraw_all")
    kb.button(text="◀️ Назад", callback_data="menu_main")
    kb.adjust(1)
    
    await state.set_state(WithdrawState.amount)
    await cq.message.edit_text(text, reply_markup=kb.as_markup())
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
        await show_main_menu(msg)
        return
    if txt in ("всё", "все"):
        bal = await get_balance(msg.from_user.id)
        await state.clear()
        await do_withdraw(msg, msg.from_user.id, bal)
        return
    try:
        amount = Decimal(txt.replace(",", ".")).quantize(Decimal("0.000000001"))
    except:
        await msg.answer("❌ Неверная сумма.")
        return
        
    bal = await get_balance(msg.from_user.id)
    if amount <= 0:
        await msg.answer("❌ Сумма > 0")
    elif amount > bal:
        await msg.answer(f"❌ У тебя только {fmt_money(bal)} BC")
    else:
        await state.clear()
        await do_withdraw(msg, msg.from_user.id, amount)

async def do_withdraw(msg: types.Message, uid: int, amount: Decimal):
    await msg.answer("⏳ Обрабатываю вывод...")
    pool = await get_pool()
    async with pool.acquire() as db:
        res = await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id = $2 AND balance >= $1", float(amount), uid)
        if res == "UPDATE 0":
            await msg.answer("❌ Недостаточно BC.")
            return

    resp = await bc_transfer(uid, amount)
    
    if resp.get("status") == "ok":
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("INSERT INTO transfers (user_id, direction, amount, transaction_id, status, created_at) VALUES ($1,'out',$2,$3,'ok',$4)", uid, amount, resp.get("transaction_id"), time.time())
        await msg.answer(f"✅ Выведено: <b>{fmt_money(amount)} BC</b>\nTX: <code>{str(resp.get('transaction_id'))[:16]}...</code>", reply_markup=InlineKeyboardBuilder().button(text="🏠 Меню", callback_data="menu_main").as_markup())
    else:
        await add_balance(uid, amount)
        await msg.answer(f"❌ Ошибка: {resp.get('error')}\nСредства возвращены.", reply_markup=InlineKeyboardBuilder().button(text="🏠 Меню", callback_data="menu_main").as_markup())

# ========== ОБМЕН (ПРОДАЖА BC ЗА ЗВЁЗДЫ) ==========
@dp.callback_query(F.data == "menu_exchange")
async def cb_exchange(cq: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="💵 Купить BC (за ⭐)", callback_data="exchange_buy")
    kb.button(text="💸 Продать BC (за ⭐)", callback_data="exchange_sell")
    kb.button(text="◀️ Назад", callback_data="menu_main")
    kb.adjust(1)
    await cq.message.edit_text("🛒 <b>Обмен</b>\n\nВыбери действие:", reply_markup=kb.as_markup())
    await cq.answer()

@dp.callback_query(F.data == "exchange_buy")
async def cb_ex_buy(cq: types.CallbackQuery):
    await cq.message.delete()
    await show_buy_bc(cq.message)
    await cq.answer()

@dp.callback_query(F.data == "exchange_sell")
async def cb_ex_sell(cq: types.CallbackQuery):
    bal = await get_balance(cq.from_user.id)
    rate = int(await get_setting("sell_rate", 1250))
    min_stars = int(await get_setting("min_sell_stars", 15))
    min_bc = rate * min_stars
    
    if bal < min_bc:
        await cq.answer(f"❌ Минимум {min_bc} BC (это {min_stars}⭐)", show_alert=True)
        return
        
    text = f"💸 <b>Продать BC за ⭐</b>\n\nКурс: {rate} BC = 1⭐\nМинимум: {min_stars}⭐ ({min_bc} BC)\n\nТвой баланс: <b>{fmt_money(bal)} BC</b>\n\nВведи сумму в ⭐ (например 15) или /cancel:"
    _states[cq.from_user.id] = "wait_sell_stars"
    await cq.message.edit_text(text)
    await cq.answer()

# ========== ПОМОЩЬ ==========
@dp.callback_query(F.data == "menu_help")
async def cb_help(cq: types.CallbackQuery):
    text = (
        "❓ <b>Помощь — описание игр</b>\n\n"
        "⚽ <b>Футбол</b>\n"
        "9️⃣⚽ Гол в девятку — x2\n"
        "⚽ Любой другой гол — x1.25\n\n"
        "🏀 <b>Баскет</b>\n"
        "🔥 Чистый гол — x2.5\n"
        "🏀 Обычный гол — x1.5\n"
        "❌ Застрял — x0.5\n\n"
        "🎯 <b>Дартс</b>\n"
        "🔥 Центр — x2.5\n"
        "⚪️ Белое кольцо — x1.6\n"
        "🔴 Красное кольцо — x1.3\n\n"
        "🎳 <b>Боулинг</b>\n"
        "🔥 Страйк — x2.2\n"
        "5️⃣ 5 кеглей — x1.88\n"
        "4️⃣ 4 кегли — x1.5\n\n"
        "🎰 <b>Слоты</b>\n"
        "7️⃣7️⃣7️⃣ — x12 | BAR BAR BAR — x7\n"
        "🍋🍋🍋 — x4 | 🍇🍇🍇 — x3\n"
        "два 7️⃣ — x2 | два BAR — x1.5\n\n"
        "🎲 <b>Кубик</b>\n"
        "⚔️ PvP дуэль — ответом на сообщение, победа → x1.9\n\n"
        "📌 Мин. ставка: 100 BC | Макс: 100000 BC"
    )
    kb = InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="menu_main").as_markup()
    try:
        await cq.message.edit_text(text, reply_markup=kb)
    except:
        await cq.message.answer(text, reply_markup=kb)
    await cq.answer()

# ========== ТОП ЧАТОВ ==========
@dp.callback_query(F.data == "menu_top")
async def cb_top(cq: types.CallbackQuery):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""
            SELECT chat_title, SUM(bet) as total 
            FROM bets WHERE chat_id IS NOT NULL
            GROUP BY chat_id, chat_title 
            ORDER BY total DESC LIMIT 10
        """)
    
    text = "🏆 <b>Топ чатов по ставкам (за всё время):</b>\n\n"
    if not rows:
        text += "Пока нет данных."
    else:
        for i, r in enumerate(rows, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            text += f"{medal} {escape_html(r['chat_title'] or 'Без названия')} — {fmt_money(r['total'])} BC\n"
            
    kb = InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="menu_main").as_markup()
    try:
        await cq.message.edit_text(text, reply_markup=kb)
    except:
        await cq.message.answer(text, reply_markup=kb)
    await cq.answer()

# ========== АДМИНКА ==========
@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="⚙️ Настройки", callback_data="adm_settings")
    kb.button(text="🎁 Создать фриспины", callback_data="adm_spins")
    kb.button(text="📈 Коэффициенты", callback_data="adm_coeffs")
    kb.button(text="🤝 Партнеры", callback_data="adm_partners")
    kb.adjust(2, 2, 1)
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        users = (await db.fetchrow("SELECT COUNT(*) as c FROM users"))["c"]
        bal = (await db.fetchrow("SELECT SUM(balance) as s FROM users"))["s"] or 0
        bets = (await db.fetchrow("SELECT COUNT(*) as c, SUM(bet) as s FROM bets"))["c"]
        vol = (await db.fetchrow("SELECT SUM(bet) as s FROM bets"))["s"] or 0
        pd = (await db.fetchrow("SELECT COUNT(*) as c FROM transfers WHERE direction='in' AND status='pending'"))["c"]
    
    text = f"📊 <b>Статистика</b>\n\n👥 Юзеров: {users}\n💰 Балансы: {fmt_money(bal)} BC\n🎲 Ставок: {bets} (оборот: {fmt_money(vol)} BC)"
    kb = InlineKeyboardBuilder().button(text="◀️ Назад", callback_data="adm_back").as_markup()
    await cq.message.edit_text(text, reply_markup=kb)
    await cq.answer()

@dp.callback_query(F.data == "adm_settings")
async def adm_settings(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    # Кнопки для изменения настроек через FSM
    kb = InlineKeyboardBuilder()
    kb.button(text="Изменить Мин. ставку", callback_data="adm_set_min_bet")
    kb.button(text="Изменить Курс покупки ⭐", callback_data="adm_set_buy_rate")
    kb.button(text="◀️ Назад", callback_data="adm_back")
    kb.adjust(1)
    await cq.message.edit_text("⚙️ <b>Настройки</b>\n(Выбери что менять)", reply_markup=kb.as_markup())
    await cq.answer()

@dp.callback_query(F.data == "adm_spins")
async def adm_spins(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_create_spins"
    await cq.message.edit_text("Введи: <b>кол-во спинов</b> <b>кол-во активаций</b> <b>ставка</b>\nПример: <code>5 30 100</code>")
    await cq.answer()

@dp.callback_query(F.data == "adm_back")
async def adm_back(cq: types.CallbackQuery):
    await cq.message.delete()
    await cmd_admin(cq.message)
    await cq.answer()

# ========== ТЕКСТОВЫЙ ОБРАБОТЧИК (СОСТОЯНИЯ) ==========
@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message, state: FSMContext):
    uid = msg.from_user.id
    st = _states.get(uid)

    if st == "wait_sell_stars":
        _states.pop(uid, None)
        try:
            stars = int(msg.text.strip())
            min_stars = int(await get_setting("min_sell_stars", 15))
            if stars < min_stars: raise ValueError()
        except:
            await msg.answer("❌ Неверное кол-во ⭐.")
            return
            
        rate = int(await get_setting("sell_rate", 1250))
        cost_bc = stars * rate
        bal = await get_balance(uid)
        
        if bal < cost_bc:
            await msg.answer("❌ Недостаточно BC.")
            return
            
        # Списываем BC, отправляем админу
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", cost_bc, uid)
            row = await db.fetchrow("INSERT INTO transfers (user_id, direction, amount, status, created_at) VALUES ($1, 'out_stars', $2, 'pending', $3) RETURNING id", uid, stars, time.time())
            wid = row["id"]
            
        await msg.answer(f"✅ Заявка #{wid} на {stars}⭐ создана. Ожидайте выплаты.")
        
        # Админу
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Оплачено", callback_data=f"adm_pay_stars_{wid}")
        kb.button(text="❌ Отклонить", callback_data=f"adm_decl_stars_{wid}")
        try:
            await bot.send_message(ADMIN_ID, f"💸 Заявка на {stars}⭐ от @{msg.from_user.username}\nСписано: {cost_bc} BC", reply_markup=kb.as_markup())
        except: pass
        return
        
    if st == "adm_create_spins":
        _states.pop(uid, None)
        try:
            parts = msg.text.split()
            spins, acts, bet = int(parts[0]), int(parts[1]), float(parts[2])
        except:
            await msg.answer("❌ Формат: 5 30 100")
            return
            
        # Отправляем сообщение (ты сам перешлёшь в чат)
        kb = InlineKeyboardBuilder()
        kb.button(text="🎁 Забрать фриспины", callback_data=f"claim_spins_0") # id=0 потом заменим
        text = f"🎉 Успейте получить {spins} FREE SPINS!\n🎁 Осталось {acts}/{acts} активаций\nСтавка: {bet} BC"
        sent = await bot.send_message(ADMIN_ID, text, reply_markup=kb.as_markup())
        
        # Сохраняем в БД
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("INSERT INTO free_spin_promos (spins_per_user, total_activations, left_activations, bet_amount, message_id, active, created_at) VALUES ($1,$2,$2,$3,$4,TRUE,$5) RETURNING id", spins, acts, bet, sent.message_id, time.time())
            pid = row["id"]
            # Обновляем callback_data
            kb = InlineKeyboardBuilder().button(text="🎁 Забрать фриспины", callback_data=f"claim_spins_{pid}")
            await bot.edit_message_reply_markup(ADMIN_ID, sent.message_id, reply_markup=kb.as_markup())
        await msg.answer("✅ Фриспины созданы! Перешли сообщение выше в нужный чат.")
        return

# ========== ПАРТНЁРКА / ЧАТЫ ==========
@dp.my_chat_member()
async def on_chat_member(event: types.ChatMemberUpdated):
    if event.new_chat_member.status == "administrator":
        adder = event.from_user
        if adder.id == int((await bot.get_me()).id): return
        
        pool = await get_pool()
        async with pool.acquire() as db:
            exists = await db.fetchrow("SELECT chat_id FROM partner_chats WHERE chat_id=$1", event.chat.id)
            if not exists:
                # Регистрируем как pending (ждем команду рег)
                await db.execute("INSERT INTO partner_chats (chat_id, chat_title, owner_id, active, created_at) VALUES ($1, $2, $3, FALSE, $4)", 
                    event.chat.id, event.chat.title, adder.id, time.time())

@dp.message(F.text.lower().in_(["рег", "/reg"]))
async def cmd_reg_chat(msg: types.Message):
    if msg.chat.type not in ("group", "supergroup"): return
    
    pool = await get_pool()
    async with pool.acquire() as db:
        chat = await db.fetchrow("SELECT * FROM partner_chats WHERE chat_id=$1", msg.chat.id)
        if not chat:
            await msg.reply("❌ Бот не добавлен как админ.")
            return
            
        if chat["owner_id"] != msg.from_user.id:
            await msg.reply("❌ Только создатель чата (или тот, кто добавил бота) может зарегистрировать беседу.")
            return
            
        if chat["active"]:
            await msg.reply("✅ Беседа уже зарегистрирована!")
            return
            
        await db.execute("UPDATE partner_chats SET active=TRUE, percent=5 WHERE chat_id=$1", msg.chat.id)
        await msg.reply("✅ <b>Беседа зарегистрирована!</b>\n\n🎉 Теперь сюда будут приходить чеки и бонусы.")

# ========== ИГРЫ В ЧАТЕ ==========
async def play_game(msg: types.Message, game: str, bet: float):
    uid = msg.from_user.id
    bal = await get_balance(uid)
    min_bet = int(await get_setting("min_bet", 100))
    max_bet = int(await get_setting("max_bet", 100000))
    
    if bet < min_bet:
        return await msg.reply(f"⚠️ Минимальная ставка: {min_bet} BC")
    if bet > max_bet:
        return await msg.reply(f"⚠️ Максимальная ставка: {max_bet} BC")
    if bal < bet:
        return await msg.reply(f"❌ Недостаточно BC (баланс: {fmt_money(bal)})")
        
    # Списание
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", bet, uid)
        
    emojis = {"футбол": "⚽", "баскет": "🏀", "дартс": "🎯", "боулинг": "🎳", "слоты": "🎰"}
    if game not in emojis: return
    
    name = msg.from_user.first_name
    await msg.reply(f"{emojis[game]} {name}, твоя ставка <b>{bet} BC</b> будет разыграна прямо сейчас!")
    
    # Анимация
    sent_dice = await bot.send_dice(msg.chat.id, emoji=emojis[game])
    await asyncio.sleep(4)
    val = sent_dice.dice.value
    
    # Логика
    mult = 0
    text = ""
    
    if game == "футбол":
        if val == 1: text = "😢 Мимо ворот"
        elif val in (2,3,4): text = "⚽ Гол!"; mult = 1.25
        elif val == 5: text = "9️⃣⚽ Гол в девятку!"; mult = 2.0
    elif game == "баскет":
        if val in (1,2): text = "😢 Мимо"
        elif val in (3,4): text = "🏀 Обычный гол!"; mult = 1.5
        elif val == 5: text = "🔥🏀 Чистый гол!"; mult = 2.5
    elif game == "дартс":
        if val in (1,2): text = "😢 Мимо"
        elif val in (3,4): text = "🔴 Красное кольцо!"; mult = 1.3
        elif val == 5: text = "⚪️ Белое кольцо!"; mult = 1.6
        elif val == 6: text = "🔥🎯 В центр!"; mult = 2.5
    elif game == "боулинг":
        if val in (1,2,3): text = "😢 Мимо"
        elif val == 4: text = "4️⃣ 4 кегли!"; mult = 1.5
        elif val == 5: text = "5️⃣ 5 кеглей!"; mult = 1.88
        elif val == 6: text = "🔥🎳 Страйк!"; mult = 2.2
    elif game == "слоты":
        # Упрощенная логика для 1-64
        if val >= 60: text = "7️⃣7️⃣7️⃣ x12"; mult = 12
        elif val >= 50: text = "BAR BAR BAR x7"; mult = 7
        elif val >= 30: text = "🍋🍋🍋 x4"; mult = 4
        elif val >= 15: text = "🍇🍇🍇 x3"; mult = 3
        elif val >= 5: text = "два 7️⃣ x2"; mult = 2
        elif val >= 2: text = "два BAR x1.5"; mult = 1.5
        else: text = "😢 Пусто"

    payout = int(bet * mult)
    
    # Запись в БД
    async with pool.acquire() as db:
        await db.execute("INSERT INTO bets (user_id, game, bet, payout, multiplier, chat_id, chat_title, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", 
            uid, game, bet, payout, mult, msg.chat.id, msg.chat.title, time.time())
            
        # Партнерка (отчисления владельцу)
        partner = await db.fetchrow("SELECT owner_id, percent FROM partner_chats WHERE chat_id=$1 AND active=TRUE", msg.chat.id)
        if partner and payout > 0:
            partner_cut = payout * (float(partner["percent"]) / 100)
            await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", partner_cut, partner["owner_id"])
            
    if payout > 0:
        await add_balance(uid, payout)
        await msg.reply(f"{text} (x{mult})\n\n{name} выиграл(а) <b>{payout} BC</b>")
    else:
        await msg.reply(f"{text}\n\n{name} проиграл(а) <b>{bet} BC</b>")

@dp.message(F.text.regexp(r"^(футбол|баскет|дартс|боулинг|слоты)\s+(\d+)$"))
async def handle_game(msg: types.Message):
    if msg.chat.type not in ("group", "supergroup"): return
    game = msg.text.split()[0].lower()
    bet = float(msg.text.split()[1])
    await play_game(msg, game, bet)

# ========== ФРИСПИНЫ (ЗАБРАТЬ И ИГРАТЬ) ==========
@dp.callback_query(F.data.startswith("claim_spins_"))
async def cb_claim_spins(cq: types.CallbackQuery):
    pid = int(cq.data.split("_")[2])
    uid = cq.from_user.id
    
    pool = await get_pool()
    async with pool.acquire() as db:
        # Проверяем не брал ли уже
        claimed = await db.fetchrow("SELECT 1 FROM promo_claims WHERE user_id=$1 AND promo_id=$2", uid, pid)
        if claimed:
            return await cq.answer("❌ Ты уже забирал эти фриспины!", show_alert=True)
            
        promo = await db.fetchrow("SELECT * FROM free_spin_promos WHERE id=$1 AND active=TRUE", pid)
        if not promo or promo["left_activations"] <= 0:
            return await cq.answer("❌ Активации закончились!", show_alert=True)
            
        # Выдаем
        await db.execute("INSERT INTO promo_claims (user_id, promo_id, claimed_at) VALUES ($1,$2,$3)", uid, pid, time.time())
        await db.execute("UPDATE free_spin_promos SET left_activations = left_activations - 1 WHERE id=$1", pid)
        await db.execute("UPDATE users SET free_spins = free_spins + $1 WHERE user_id=$2", promo["spins_per_user"], uid)
        
        left = promo["left_activations"] - 1
        text = f"🎉 Успейте получить {promo['spins_per_user']} FREE SPINS!\n🎁 Осталось {left}/{promo['total_activations']} активаций\nСтавка: {promo['bet_amount']} BC"
        try:
            await cq.message.edit_text(text, reply_markup=cq.message.reply_markup)
        except: pass
        
    await cq.answer(f"✅ Получено {promo['spins_per_user']} фриспинов!")

# (Упрощенно) игра на фриспин
@dp.message(F.text.regexp(r"^(футбол|баскет|дартс|боулинг|слоты)\s+фриспин$"))
async def handle_freespin(msg: types.Message):
    if msg.chat.type not in ("group", "supergroup"): return
    uid = msg.from_user.id
    game = msg.text.split()[0].lower()
    
    pool = await get_pool()
    async with pool.acquire() as db:
        u = await db.fetchrow("SELECT free_spins FROM users WHERE user_id=$1", uid)
        if not u or u["free_spins"] <= 0:
            return await msg.reply("❌ У тебя нет фриспинов.")
            
        await db.execute("UPDATE users SET free_spins = free_spins - 1 WHERE user_id=$1", uid)
    
    # Запускаем игру с фиксированной ставкой (100), но не списываем баланс
    await play_game(msg, game, 100)
    await msg.reply("🎁 (Сыграно за фриспин)")

# ========== ЗАПУСК ==========
async def health(_):
    return web.Response(text="ok")

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
