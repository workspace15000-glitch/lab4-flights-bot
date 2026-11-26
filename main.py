import asyncio
import os
import re
import sqlite3
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from time import monotonic
from typing import NamedTuple
import httpx
from telegram import LinkPreviewOptions, ReplyKeyboardMarkup, Update
from telegram.constants import ParseMode


API_URL = "https://api.rasp.yandex-net.ru/v3.0/schedule/"
DATABASE = Path(__file__).with_name("flight_settings.sqlite3")
AIRPORTS = {"SVO": "Шереметьево", "VKO": "Внуково", "DME": "Домодедово"}
AIRPORT_NAMES = {name.upper(): code for code, name in AIRPORTS.items()}
DEFAULT_AIRPORT, DEFAULT_LIMIT, MAX_LIMIT = "SVO", 5, 10
MOSCOW = timezone(timedelta(hours=3))
CACHE_SECONDS = 180
DEPARTURES, ARRIVALS, SETTINGS = "Вылеты", "Прилёты", "Настройки"
MENU = ReplyKeyboardMarkup([[DEPARTURES, ARRIVALS], [SETTINGS]], resize_keyboard=True)
AIRPORT_LIST = ", ".join(f"{code} ({name})" for code, name in AIRPORTS.items())
HELP = (
    f"Расписание московских аэропортов. Время московское.\n\nАэропорты: {AIRPORT_LIST}.\n"
    "Введите код, название аэропорта или /airport VKO.\n"
    "Вылеты — /departures, прилёты — /arrivals.\n"
    "Поиск по номеру на сегодня — /flight SU100.\n"
    f"Число рейсов в выдаче от 1 до {MAX_LIMIT} — /limit 3.\n"
    "Настройки — /settings."
)
SOURCE = '<a href="https://rasp.yandex.ru/">Данные предоставлены сервисом Яндекс.Расписания</a>'


class Flight(NamedTuple):
    number: str
    route: str
    scheduled: datetime
    terminal: str


def parse_flights(data, event):
    rows = data.get("schedule") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError("В ответе отсутствует расписание")
    flights = []
    for row in rows:
        thread = row.get("thread") if isinstance(row, dict) else None
        if not isinstance(thread, dict) or thread.get("transport_type", "plane") != "plane":
            continue
        number = thread.get("number")
        route = thread.get("short_title") or thread.get("title")
        value = row.get(event)
        if not all(isinstance(field, str) and field.strip() for field in (number, route, value)):
            continue
        try:
            scheduled = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if scheduled.tzinfo is None:
                continue
            scheduled = scheduled.astimezone(MOSCOW)
        except (ValueError, OverflowError):
            continue
        terminal = row.get("terminal")
        terminal = terminal.strip()[:20] if isinstance(terminal, str) else ""
        flights.append(Flight(number.strip()[:24], route.strip()[:150], scheduled, terminal))
    if rows and not flights:
        raise ValueError("В расписании нет корректных записей")
    return sorted(flights, key=lambda flight: flight.scheduled)


flight_cache = {}
airport_locks = defaultdict(asyncio.Lock)


async def fetch_flights(airport, event, day):
    key = (airport, event, day)
    async with airport_locks[(airport, event)]:
        cached = flight_cache.get(key)
        if cached and monotonic() - cached[0] < CACHE_SECONDS:
            return cached[1]
        for old_key, (updated, _) in list(flight_cache.items()):
            if monotonic() - updated >= CACHE_SECONDS:
                del flight_cache[old_key]
        params = {
            "apikey": os.environ.get("RASP_KEY", ""), "station": airport, "system": "iata",
            "transport_types": "plane", "event": event, "date": day.isoformat(),
            "result_timezone": "Europe/Moscow", "format": "json", "lang": "ru_RU", "limit": 100, "offset": 0,
        }
        flights = []
        async with httpx.AsyncClient(timeout=10) as client:
            while True:
                response = await client.get(API_URL, params=params)
                response.raise_for_status()
                data = response.json()
                flights.extend(parse_flights(data, event))
                rows = data["schedule"]
                next_offset = params["offset"] + len(rows)
                page = data.get("pagination", {})
                if not isinstance(page, dict):
                    raise ValueError("Неверные параметры страниц расписания")
                total = page.get("total", next_offset)
                offset = page.get("offset", 0)
                if type(total) is not int or total < 0 or offset != params["offset"]:
                    raise ValueError("Неверные параметры страниц расписания")
                if next_offset >= total:
                    break
                if not rows or next_offset >= 3000:
                    raise ValueError("Не удалось получить все страницы расписания")
                params["offset"] = next_offset
        flights.sort(key=lambda flight: flight.scheduled)
        flight_cache[key] = monotonic(), flights
        return flights


def preferences(user_id, airport=None, limit=None):
    with closing(sqlite3.connect(DATABASE, timeout=1)) as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS settings (
            user_id INTEGER PRIMARY KEY, airport TEXT NOT NULL, flight_limit INTEGER NOT NULL)""")
        if airport is not None or limit is not None:
            db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT airport, flight_limit FROM settings WHERE user_id = ?", (user_id,)).fetchone()
        saved_airport, saved_limit = row or (DEFAULT_AIRPORT, DEFAULT_LIMIT)
        if saved_airport not in AIRPORTS:
            saved_airport = DEFAULT_AIRPORT
        if not isinstance(saved_limit, int) or not 1 <= saved_limit <= MAX_LIMIT:
            raise sqlite3.DatabaseError("Некорректные настройки пользователя")
        if airport is not None or limit is not None:
            saved_airport = saved_airport if airport is None else airport
            saved_limit = saved_limit if limit is None else limit
            db.execute("REPLACE INTO settings VALUES (?, ?, ?)", (user_id, saved_airport, saved_limit))
        return saved_airport, saved_limit


def normalize_number(number):
    return re.sub(r"[\s-]", "", number.upper())


def flight_line(flight):
    time = flight.scheduled.strftime("%d.%m %H:%M")
    terminal = f" · терминал {escape(flight.terminal)}" if flight.terminal else ""
    return f"{time} · {escape(flight.number)} · {escape(flight.route)}{terminal}"


async def show_board(update, event=None, number=None):
    airport, limit = preferences(update.effective_user.id)
    now = datetime.now(MOSCOW)
    today = now.date()
    airport_label = f"{AIRPORTS[airport]} ({airport})"
    try:
        if number is not None:
            arrivals, departures = await asyncio.gather(
                fetch_flights(airport, "arrival", today), fetch_flights(airport, "departure", today)
            )
            found = [flight for flight in arrivals + departures if normalize_number(flight.number) == number]
            found.sort(key=lambda flight: flight.scheduled)
            title = f"Поиск {number} на сегодня · {airport_label}"
        else:
            flights = await fetch_flights(airport, event, today)
            found = [flight for flight in flights if flight.scheduled >= now]
            if len(found) < limit:
                found.extend(await fetch_flights(airport, event, today + timedelta(days=1)))
            label = DEPARTURES if event == "departure" else ARRIVALS
            title = f"{label} · {airport_label}"
    except (httpx.HTTPError, ValueError) as error:
        message = "Не удалось получить расписание. Попробуйте чуть позже."
        if isinstance(error, httpx.HTTPStatusError):
            if error.response.status_code in (401, 403):
                message = "Яндекс отклонил ключ API. Проверьте RASP_KEY и перезапустите бота."
            elif error.response.status_code == 429:
                message = "Достигнут лимит запросов Яндекс Расписаний. Попробуйте позже."
        await update.message.reply_text(message)
        return

    lines = [f"{title}\nВремя по расписанию, МСК"]
    lines.extend(flight_line(flight) for flight in found[:limit])
    if not found:
        lines.append("В загруженном табло подходящих рейсов нет.")
    lines.append(SOURCE)
    await update.message.reply_text("\n\n".join(lines), parse_mode=ParseMode.HTML,
                                    link_preview_options=LinkPreviewOptions(is_disabled=True))


async def show_settings(update, **changes):
    airport, limit = preferences(update.effective_user.id, **changes)
    heading = "Настройки сохранены" if changes else SETTINGS
    await update.message.reply_text(f"{heading}\nАэропорт: {AIRPORTS[airport]} ({airport})\nРейсов в выдаче: {limit}")


async def handle(update: Update, context):
    text = update.message.text.strip()
    if text.startswith("/"):
        command = text.split(maxsplit=1)[0].split("@")[0].lower()
        argument = " ".join(context.args or []).upper()
    else:
        command, argument = text, text.upper()
    try:
        if command in ("/start", "/help"):
            await update.message.reply_text(f"{HELP}\n\n{SOURCE}", reply_markup=MENU, parse_mode=ParseMode.HTML,
                                            link_preview_options=LinkPreviewOptions(is_disabled=True))
        elif command in ("/departures", DEPARTURES):
            await show_board(update, "departure")
        elif command in ("/arrivals", ARRIVALS):
            await show_board(update, "arrival")
        elif command in ("/settings", SETTINGS):
            await show_settings(update)
        elif command == "/limit":
            if not re.fullmatch(r"[0-9]{1,2}", argument) or not 1 <= int(argument) <= MAX_LIMIT:
                await update.message.reply_text(f"Укажите число от 1 до {MAX_LIMIT}. Пример: /limit 3.")
            else:
                await show_settings(update, limit=int(argument))
        elif command == "/flight":
            number = normalize_number(argument)
            if re.fullmatch(r"[A-ZА-ЯЁ0-9]{2,12}", number) and any(char.isdigit() for char in number):
                await show_board(update, number=number)
            else:
                await update.message.reply_text("Введите номер рейса. Пример: /flight SU100.")
        else:
            airport = AIRPORT_NAMES.get(argument, argument)
            if airport in AIRPORTS:
                await show_settings(update, airport=airport)
            else:
                await update.message.reply_text(f"Выберите аэропорт. Доступные коды: {AIRPORT_LIST}.", reply_markup=MENU)
    except sqlite3.Error:
        await update.message.reply_text("Не удалось прочитать или сохранить настройки. Попробуйте позже.")
