#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎯 Avito Hunter — Render Server
FastAPI + Aiogram 3.  SQLite база.  Heartbeat воркера.
"""

import os
import sqlite3
import asyncio
import signal
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import List, Optional, Dict

from fastapi import FastAPI, Header, HTTPException, Depends
from pydantic import BaseModel
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
import uvicorn

# ═══════════════════════════════════════════════════════════════
# ⚙️ КОНФИГ
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
API_SECRET  = os.environ.get("API_SECRET", "change-me-secret")
DB_PATH     = "avito_hunter.db"

HEARTBEAT_TIMEOUT = 600   # 10 мин — воркер считается оффлайн
HEARTBEAT_CHECK   = 60  # проверка каждую минуту

if not BOT_TOKEN:
    raise RuntimeError("❌ Задай BOT_TOKEN в переменных окружения Render!")

# ═══════════════════════════════════════════════════════════════
# 📦 МОДЕЛИ
# ═══════════════════════════════════════════════════════════════

class AvitoItem(BaseModel):
    avito_id: str
    title: str
    price: int
    price_text: str
    url: str
    image_url: Optional[str] = None
    location: Optional[str] = None

class TaskResult(BaseModel):
    task_id: int
    chat_id: int
    items: List[AvitoItem]

class AnalysisResult(BaseModel):
    task_id: int
    chat_id: int
    stats: dict
    items: List[AvitoItem]

class BlockedReport(BaseModel):
    task_id: int
    reason: str

# ═══════════════════════════════════════════════════════════════
# 🗄️ SQLITE  (async-обёртки)
# ═══════════════════════════════════════════════════════════════

def _init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            avito_url TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS items (
            avito_id TEXT PRIMARY KEY,
            task_id INTEGER,
            chat_id INTEGER,
            title TEXT,
            price INTEGER,
            url TEXT,
            image_url TEXT,
            location TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS analysis_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            avito_url TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            result_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS user_settings (
            chat_id INTEGER PRIMARY KEY,
            monitoring_enabled INTEGER DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS worker_status (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_heartbeat TIMESTAMP,
            is_online INTEGER DEFAULT 0,
            last_notified_state TEXT DEFAULT 'offline'
        )
    """)
    c.execute("INSERT OR IGNORE INTO worker_status (id) VALUES (1)")
    conn.commit()
    conn.close()

async def db_execute(query: str, params=()):
    def _run():
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(query, params)
        conn.commit()
        rid = c.lastrowid
        conn.close()
        return rid
    return await asyncio.to_thread(_run)

async def db_fetchall(query: str, params=()):
    def _run():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    return await asyncio.to_thread(_run)

async def db_fetchone(query: str, params=()):
    rows = await db_fetchall(query, params)
    return rows[0] if rows else None

# ═══════════════════════════════════════════════════════════════
# 🤖 TELEGRAM
# ═══════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

pending_links: Dict[int, str] = {}

async def tg_send(chat_id: int, text: str, image: Optional[str] = None):
    try:
        if image and image.startswith("http"):
            await bot.send_photo(chat_id, photo=image, caption=text, parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
    except Exception as e:
        print(f"[!] TG error: {e}")

# ─── Клавиатуры ──────────────────────────────────────────────

def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📡 Начать мониторинг", callback_data="menu_monitor")],
        [InlineKeyboardButton(text="📊 Анализ рынка",      callback_data="menu_analysis")],
        [InlineKeyboardButton(text="📋 Мои подписки",      callback_data="menu_list")],
        [InlineKeyboardButton(text="⏹ Остановить всё",     callback_data="menu_stop")],
    ])

def kb_action(url: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📡 Мониторить цены",   callback_data="act_monitor")],
        [InlineKeyboardButton(text="📊 Анализировать рынок", callback_data="act_analysis")],
    ])

# ═══════════════════════════════════════════════════════════════
# 🚀 FASTAPI
# ═══════════════════════════════════════════════════════════════

def verify_secret(x_secret: str = Header(...)):
    if x_secret != API_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    return x_secret

@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_db()
    asyncio.create_task(dp.start_polling(bot))
    asyncio.create_task(heartbeat_monitor())
    yield
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

# ─── Heartbeat ─────────────────────────────────────────────────

@app.post("/worker_online")
async def worker_online(x_secret: str = Depends(verify_secret)):
    await db_execute(
        "UPDATE worker_status SET is_online=1, last_heartbeat=CURRENT_TIMESTAMP, last_notified_state='online' WHERE id=1"
    )
    chats = await db_fetchall("SELECT DISTINCT chat_id FROM tasks WHERE status='active'")
    for row in chats:
        await tg_send(row["chat_id"],
            "<b>▶️ Воркер подключён!</b>\n"
            "Планшет на связи. Нажми <b>«Начать мониторинг»</b>, чтобы запустить парсинг.")
    return {"ok": True}

@app.post("/worker_offline")
async def worker_offline(x_secret: str = Depends(verify_secret)):
    await db_execute(
        "UPDATE worker_status SET is_online=0, last_notified_state='offline' WHERE id=1"
    )
    chats = await db_fetchall("SELECT DISTINCT chat_id FROM tasks WHERE status='active'")
    for row in chats:
        await tg_send(row["chat_id"],
            "<b>⏹ Парсинг остановлен</b>\nПланшет выключен или потерял связь.")
    return {"ok": True}

@app.post("/heartbeat")
async def heartbeat(x_secret: str = Depends(verify_secret)):
    await db_execute(
        "UPDATE worker_status SET last_heartbeat=CURRENT_TIMESTAMP, is_online=1 WHERE id=1"
    )
    return {"ok": True}

async def heartbeat_monitor():
    while True:
        await asyncio.sleep(HEARTBEAT_CHECK)
        try:
            row = await db_fetchone("SELECT * FROM worker_status WHERE id=1")
            if not row:
                continue
            last = row["last_heartbeat"]
            online = row["is_online"]
            state = row["last_notified_state"] or "offline"
            now = datetime.utcnow()
            hb = datetime.fromisoformat(last) if last else now - timedelta(seconds=HEARTBEAT_TIMEOUT + 1)
            offline = (now - hb).total_seconds() > HEARTBEAT_TIMEOUT

            if offline and online:
                await db_execute("UPDATE worker_status SET is_online=0, last_notified_state='offline' WHERE id=1")
                chats = await db_fetchall("SELECT DISTINCT chat_id FROM tasks WHERE status='active'")
                for r in chats:
                    await tg_send(r["chat_id"],
                        "<b>⏹ Парсинг остановлен</b>\n"
                        "Планшет не отвечает больше 10 минут. Включи его, чтобы возобновить.")
            elif not offline and not online:
                await db_execute("UPDATE worker_status SET is_online=1, last_notified_state='online' WHERE id=1")
                chats = await db_fetchall("SELECT DISTINCT chat_id FROM tasks WHERE status='active'")
                for r in chats:
                    await tg_send(r["chat_id"],
                        "<b>▶️ Воркер снова на связи!</b>\n"
                        "Планшет включён. Нажми <b>«Начать мониторинг»</b> для продолжения.")
        except Exception as e:
            print(f"[!] HB monitor: {e}")

# ─── Tasks API ─────────────────────────────────────────────────

@app.get("/get_task")
async def get_task(x_secret: str = Depends(verify_secret)):
    row = await db_fetchone("""
        SELECT t.id, t.chat_id, t.avito_url FROM tasks t
        JOIN user_settings u ON t.chat_id = u.chat_id
        WHERE t.status='active' AND u.monitoring_enabled=1
        ORDER BY t.updated_at ASC LIMIT 1
    """)
    if row:
        await db_execute("UPDATE tasks SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (row["id"],))
        return {"task_id": row["id"], "chat_id": row["chat_id"], "url": row["avito_url"]}
    return {"task_id": None}

@app.get("/get_analysis_task")
async def get_analysis_task(x_secret: str = Depends(verify_secret)):
    row = await db_fetchone("""
        SELECT id, chat_id, avito_url FROM analysis_tasks
        WHERE status='pending' ORDER BY created_at ASC LIMIT 1
    """)
    if row:
        await db_execute("UPDATE analysis_tasks SET status='processing' WHERE id=?", (row["id"],))
        return {"task_id": row["id"], "chat_id": row["chat_id"], "url": row["avito_url"]}
    return {"task_id": None}

@app.post("/post_results")
async def post_results(data: TaskResult, x_secret: str = Depends(verify_secret)):
    for it in data.items:
        old = await db_fetchone("SELECT price FROM items WHERE avito_id=?", (it.avito_id,))
        if old is None:
            await db_execute("""
                INSERT INTO items (avito_id,task_id,chat_id,title,price,url,image_url,location)
                VALUES (?,?,?,?,?,?,?,?)
            """, (it.avito_id, data.task_id, data.chat_id, it.title, it.price,
                  it.url, it.image_url, it.location))
            await tg_send(data.chat_id,
                f"🆕 <b>Новое объявление!</b>\n\n"
                f"📱 {it.title}\n"
                f"💰 {it.price_text}\n"
                f"📍 {it.location or '—'}\n"
                f"🔗 <a href='{it.url}'>Открыть на Авито</a>",
                it.image_url)
        elif it.price < old["price"]:
            await db_execute("UPDATE items SET price=?, last_seen=CURRENT_TIMESTAMP WHERE avito_id=?",
                           (it.price, it.avito_id))
            await tg_send(data.chat_id,
                f"🔥 <b>Цена снизилась!</b>\n\n"
                f"📱 {it.title}\n"
                f"💰 <s>{old['price']:,} ₽</s> → <b>{it.price:,} ₽</b>\n"
                f"📍 {it.location or '—'}\n"
                f"🔗 <a href='{it.url}'>Открыть на Авито</a>",
                it.image_url)
        else:
            await db_execute("UPDATE items SET last_seen=CURRENT_TIMESTAMP WHERE avito_id=?",
                           (it.avito_id,))
    return {"ok": True}

@app.post("/post_analysis")
async def post_analysis(data: AnalysisResult, x_secret: str = Depends(verify_secret)):
    s = data.stats
    trend = (
        "🔥 Рынок активно сбрасывает цены — шанс поймать удачный лот высокий!"
        if s["spread_percent"] > 20 else
        "📊 Цены стабильны. Торгуйся за каждую тысячу — перекупы не должны терпеть убытки."
    )
    text = (
        f"<b>📊 АНАЛИЗ РЫНКА АВИТО</b>\n\n"
        f"<i>Просканировано {s['count']} объявлений по вашему фильтру</i>\n\n"
        f"<b>💰 Рыночная цена:</b> {s['market_price']:,} ₽\n\n"
        f"<b>🛒 Покупай до:</b> {s['buy_price']:,} ₽\n"
        f"<b>📈 Продавай за:</b> {s['sell_price']:,} ₽\n"
        f"<b>⚡ Маржа:</b> {s['margin']:,} ₽\n\n"
        f"<b>🎯 Вероятность найти лот за {s['buy_price']:,} ₽:</b> {s['probability']}%\n\n"
        f"<b>📉 Разброс цен:</b> {s['min_price']:,} ₽ – {s['max_price']:,} ₽ ({s['spread_percent']}%)\n\n"
        f"<i>{trend}</i>"
    )
    await tg_send(data.chat_id, text)
    await db_execute("UPDATE analysis_tasks SET status='done', result_json=? WHERE id=?",
                     (str(s), data.task_id))
    return {"ok": True}

@app.post("/report_blocked")
async def report_blocked(data: BlockedReport, x_secret: str = Depends(verify_secret)):
    row = await db_fetchone("SELECT chat_id FROM tasks WHERE id=?", (data.task_id,))
    if row:
        await tg_send(row["chat_id"],
            "<b>⚠️ Твой IP получил капчу / блокировку!</b>\n\n"
            f"Причина: {data.reason}\n\n"
            "<b>🔧 Что делать:</b>\n"
            "• Перезагрузи роутер\n"
            "• Или включи авиарежим на 10 сек\n"
            "• Подожди 15–30 минут")
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# 💬 TELEGRAM HANDLERS
# ═══════════════════════════════════════════════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await msg.answer(
        "<b>🎯 Avito Hunter</b> — бот для перекупов\n\n"
        "Отправь мне <b>ссылку на поиск Авито</b> (с уже настроенными фильтрами), "
        "и я покажу, сколько можно заработать.\n\n"
        "<b>Две функции:</b>\n"
        "• 📡 <b>Мониторинг</b> — слежу за новыми лотами и снижением цен\n"
        "• 📊 <b>Анализ рынка</b> — считаю маржу, вероятность снижения, рыночную цену\n\n"
        "Нажми кнопку ниже или просто пришли ссылку 👇",
        reply_markup=kb_main(), parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data == "menu_monitor")
async def cb_menu_monitor(cb: types.CallbackQuery):
    await cb.message.answer(
        "📡 <b>Режим мониторинга</b>\n"
        "Пришли ссылку с Авито. Я буду проверять её каждые 5–7 минут и сообщать о новых объявлениях.",
        parse_mode=ParseMode.HTML
    )
    await cb.answer()

@dp.callback_query(F.data == "menu_analysis")
async def cb_menu_analysis(cb: types.CallbackQuery):
    await cb.message.answer(
        "📊 <b>Режим анализа рынка</b>\n"
        "Пришли ссылку с Авито. Я просканирую ~30 объявлений и рассчитаю:\n"
        "• Рыночную цену\n"
        "• За сколько покупать\n"
        "• За сколько продавать\n"
        "• Маржу и вероятность снижения",
        parse_mode=ParseMode.HTML
    )
    await cb.answer()

@dp.callback_query(F.data == "menu_list")
async def cb_menu_list(cb: types.CallbackQuery):
    rows = await db_fetchall(
        "SELECT id, avito_url FROM tasks WHERE chat_id=? AND status='active'",
        (cb.from_user.id,)
    )
    if not rows:
        await cb.message.answer("📭 У тебя нет активных подписок.", parse_mode=ParseMode.HTML)
    else:
        txt = "<b>📋 Активные подписки:</b>\n\n"
        for r in rows:
            txt += f"• <a href='{r['avito_url']}'>{r['avito_url'][:55]}...</a>\n"
        await cb.message.answer(txt, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    await cb.answer()

@dp.callback_query(F.data == "menu_stop")
async def cb_menu_stop(cb: types.CallbackQuery):
    await db_execute("UPDATE tasks SET status='stopped' WHERE chat_id=? AND status='active'",
                     (cb.from_user.id,))
    await db_execute("UPDATE user_settings SET monitoring_enabled=0 WHERE chat_id=?",
                     (cb.from_user.id,))
    await cb.message.answer("⏹ <b>Все подписки остановлены.</b>", parse_mode=ParseMode.HTML)
    await cb.answer()

@dp.message(F.text.contains("avito.ru"))
async def handle_link(msg: types.Message):
    url = msg.text.strip()
    pending_links[msg.chat.id] = url
    await msg.answer(
        "🔗 <b>Ссылка получена</b>\nЧто делаем? 👇",
        reply_markup=kb_action(url),
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data == "act_monitor")
async def cb_act_monitor(cb: types.CallbackQuery):
    cid = cb.from_user.id
    url = pending_links.pop(cid, None)
    if not url:
        await cb.answer("Ссылка устарела, пришли заново", show_alert=True)
        return
    await db_execute("INSERT INTO tasks (chat_id, avito_url) VALUES (?,?)", (cid, url))
    await db_execute("INSERT OR REPLACE INTO user_settings (chat_id, monitoring_enabled) VALUES (?,1)", (cid,))
    await cb.message.answer(
        "✅ <b>Мониторинг запущен!</b>\n\n"
        "Я буду следить за этой ссылкой.\n"
        "Как только планшет подключится — начнётся парсинг.",
        reply_markup=kb_main(), parse_mode=ParseMode.HTML
    )
    await cb.answer()

@dp.callback_query(F.data == "act_analysis")
async def cb_act_analysis(cb: types.CallbackQuery):
    cid = cb.from_user.id
    url = pending_links.pop(cid, None)
    if not url:
        await cb.answer("Ссылка устарела, пришли заново", show_alert=True)
        return
    await db_execute("INSERT INTO analysis_tasks (chat_id, avito_url) VALUES (?,?)", (cid, url))
    await cb.message.answer(
        "📊 <b>Анализ запущен!</b>\n\n"
        "Воркер на планшете просканирует ~30 объявлений.\n"
        "Результат придёт через 30–60 секунд...",
        reply_markup=kb_main(), parse_mode=ParseMode.HTML
    )
    await cb.answer()

@dp.message(F.text)
async def handle_text(msg: types.Message):
    await msg.answer("Пришли ссылку на Авито или используй кнопки ниже 👇", reply_markup=kb_main())

# ═══════════════════════════════════════════════════════════════
# 🏁 СТАРТ
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
