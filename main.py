import asyncio
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import NamedTuple
import httpx


API_URL = "https://api.rasp.yandex-net.ru/v3.0/schedule/"


AIRPORTS = {"SVO": "Шереметьево", "VKO": "Внуково", "DME": "Домодедово"}


MOSCOW = timezone(timedelta(hours=3))
CACHE_SECONDS = 180


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
