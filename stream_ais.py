"""
stream_ais.py
-----------------------
Streams AIS position reports (types 1, 2, 3) from aisstream.io globally
and writes one CSV file per UTC hour.

Usage:
    python stream_ais.py --api-key YOUR_KEY
    python stream_ais.py --api-key YOUR_KEY --date 2024-06-15
    python stream_ais.py --api-key YOUR_KEY --limit 1000   # test run

Output:
    data/ais_YYYYMMDD_hourHH.csv  (one file per UTC hour of data received)

Dependencies:
    pip install -r requirements-min.txt
"""

import argparse
import asyncio
import csv
import json
import logging
import sys
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import websockets
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ['AISSTREAM_API_KEY']


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("ais_collector")


# ---------------------------------------------------------------------------
# CSV columns — matches the ais_stream Postgres table (serial_id is DB-generated)
# ---------------------------------------------------------------------------

FIELDNAMES = [
    "mmsi",
    "ship_name",
    "latitude",
    "longitude",
    "time_utc",
    "message_id",
    "callsign",
    "fix_type",
    "cog",
    "sog",
    "true_heading",
    "communication_state",
    "destination",
    "imo_number",
    "navigational_status",
    "rate_of_turn",
    "message_type",
]

POSITION_MSG_TYPES = {1, 2, 3}

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"


# ---------------------------------------------------------------------------
# Hourly CSV manager
# ---------------------------------------------------------------------------

class HourlyCsvManager:
    """Opens one CSV file per UTC hour, named ais_YYYYMMDD_hourHH.csv."""

    def __init__(self, output_dir: Path, date_str: str):
        self.output_dir = output_dir
        self.date_str = date_str
        self._handles: dict[int, tuple] = {}   # hour -> (file_handle, DictWriter)
        self._counts: dict[int, int] = {}

    def _open_hour(self, hour: int):
        path = self.output_dir / f"ais_{self.date_str}_hour{hour:02d}.csv"
        fh = open(path, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        self._handles[hour] = (fh, writer)
        self._counts[hour] = 0
        log.info("Opened CSV for hour %02d UTC: %s", hour, path)

    def write(self, hour: int, row: dict):
        if hour not in self._handles:
            self._open_hour(hour)
        fh, writer = self._handles[hour]
        writer.writerow(row)
        self._counts[hour] += 1
        # Flush to disk every 500 rows so data survives an interrupt
        if self._counts[hour] % 500 == 0:
            fh.flush()

    def close_all(self):
        for hour, (fh, _) in self._handles.items():
            fh.flush()
            fh.close()
        total = sum(self._counts.values())
        log.info("Closed all CSV files. Total rows written: %d", total)
        for hour in sorted(self._counts):
            log.info("  hour %02d UTC -> %d rows", hour, self._counts[hour])


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------

def parse_position_message(raw: dict) -> Optional[tuple[int, dict]]:
    """
    Parse an aisstream.io WebSocket message.
    Returns (utc_hour, row_dict) for position reports, or None to skip.
    Column names match the ais_stream Postgres table.
    """
    msg_type_key = raw.get("MessageType", "")
    metadata = raw.get("MetaData", {})
    payload = raw.get("Message", {}).get(msg_type_key, {})

    try:
        message_id = int(payload.get("MessageID", -1))
    except (TypeError, ValueError):
        return None

    if message_id not in POSITION_MSG_TYPES:
        return None

    time_utc_str = metadata.get("time_utc") or datetime.now(timezone.utc).isoformat()
    try:
        time_utc = datetime.fromisoformat(time_utc_str.replace("Z", "+00:00"))
    except Exception:
        time_utc = datetime.now(timezone.utc)

    row = {
        "mmsi":                 metadata.get("MMSI") or payload.get("UserID"),
        "ship_name":            metadata.get("ShipName", "").strip() or None,
        "latitude":             payload.get("Latitude"),
        "longitude":            payload.get("Longitude"),
        "time_utc":             time_utc.isoformat(),
        "message_id":           message_id,
        "callsign":             metadata.get("CallSign", "").strip() or None,
        "fix_type":             payload.get("FixType"),
        "cog":                  payload.get("Cog"),
        "sog":                  payload.get("Sog"),
        "true_heading":         payload.get("TrueHeading"),
        "communication_state":  payload.get("CommunicationState"),
        "destination":          payload.get("Destination", "").strip() or None,
        "imo_number":           str(payload.get("ImoNumber", "") or "").strip() or None,
        "navigational_status":  payload.get("NavigationalStatus"),
        "rate_of_turn":         payload.get("RateOfTurn"),
        "message_type":         msg_type_key,
    }

    return time_utc.hour, row


# ---------------------------------------------------------------------------
# WebSocket streamer
# ---------------------------------------------------------------------------

async def stream(api_key: str, target_date: date, output_dir: Path, limit: Optional[int]):
    date_str = target_date.strftime("%Y%m%d")
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_mgr = HourlyCsvManager(output_dir, date_str)

    subscribe = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FilterMessageTypes": ["PositionReport"],
    }

    total = 0

    log.info("Connecting to %s", AISSTREAM_URL)
    log.info("Capturing: position reports (types 1, 2, 3) | global coverage")
    log.info("Output dir: %s", output_dir.resolve())
    log.info("Press Ctrl+C to stop.")

    # No custom signal handling — let KeyboardInterrupt bubble up through
    # asyncio.run() naturally, exactly like the reference implementation.
    try:
        while True:
            try:
                async with websockets.connect(AISSTREAM_URL, ping_interval=30, ping_timeout=60) as ws:
                    await ws.send(json.dumps(subscribe))
                    log.info("Subscribed. Streaming…")

                    async for raw_msg in ws:
                        try:
                            data = json.loads(raw_msg)
                        except json.JSONDecodeError:
                            continue

                        if "Error" in data:
                            log.error("Server error: %s", data["Error"])
                            return

                        result = parse_position_message(data)
                        if result is None:
                            continue

                        hour, row = result
                        csv_mgr.write(hour, row)
                        total += 1

                        if total % 1000 == 0:
                            log.info("Messages received: %d", total)

                        if limit and total >= limit:
                            log.info("Reached --limit of %d. Stopping.", limit)
                            return

            except websockets.exceptions.ConnectionClosedError as e:
                log.warning("Connection closed: %s — reconnecting in 5s", e)
                await asyncio.sleep(5)
            except OSError as e:
                log.warning("Network error: %s — reconnecting in 5s", e)
                await asyncio.sleep(5)

    except KeyboardInterrupt:
        log.info("Interrupted.")
    finally:
        csv_mgr.close_all()
        log.info("Done. Total position reports processed: %d", total)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Stream AIS position reports (types 1/2/3) from aisstream.io "
            "and save to hourly CSV files."
        )
    )
    parser.add_argument(
        "--date", default=date.today().isoformat(),
        help="Date label used in output filenames (YYYY-MM-DD). Default: today."
    )
    parser.add_argument(
        "--output-dir", default="data",
        help="Directory to write CSV files into. Default: ./data/"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Stop after N messages. Useful for quick testing."
    )
    args = parser.parse_args()

    try:
        target_date = date.fromisoformat(args.date)
    except ValueError:
        log.error("Invalid date: %s (expected YYYY-MM-DD)", args.date)
        sys.exit(1)

    asyncio.run(stream(
        api_key=API_KEY,
        target_date=target_date,
        output_dir=Path(args.output_dir),
        limit=args.limit,
    ))


if __name__ == "__main__":
    main()