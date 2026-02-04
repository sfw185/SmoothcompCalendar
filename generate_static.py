#!/usr/bin/env python3
"""
Static Site Generator for Smoothcomp Calendar

Scrapes events and generates static files for GitHub Pages hosting.

Usage:
    python generate_static.py              # Full scrape
    python generate_static.py --limit 20   # Quick test with 20 events
    python generate_static.py --help
"""

import argparse
import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path

from scraper import SmoothcompScraper, Event
from calendar_gen import generate_ical


# Output directory
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "static"))


def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text


async def main(limit: int | None = None):
    print(f"Output directory: {OUTPUT_DIR}")
    if limit:
        print(f"Test mode: limiting to {limit} events")
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "calendars").mkdir(exist_ok=True)

    # Scrape all events
    print("\nScraping events...")
    events: list[Event] = []

    async with SmoothcompScraper(rate_limit=0.3) as scraper:
        async for event, current, total, is_new in scraper.scrape_events_iter(max_events=limit):
            # Skip past events
            if event.start_date:
                event_start = event.start_date.replace(tzinfo=None) if event.start_date.tzinfo else event.start_date
                if event_start < datetime.now():
                    continue

            events.append(event)

            if current % 50 == 0 or current == total:
                print(f"  Progress: {current}/{total} ({len(events)} future events)")

    print(f"\nCollected {len(events)} upcoming events")

    # Group events by country
    events_by_country: dict[str, list[Event]] = {}
    for event in events:
        country = event.country or "Unknown"
        if country not in events_by_country:
            events_by_country[country] = []
        events_by_country[country].append(event)

    # Sort countries by event count
    countries_sorted = sorted(
        events_by_country.keys(),
        key=lambda c: len(events_by_country[c]),
        reverse=True
    )

    # Generate metadata.json
    print("\nGenerating metadata.json...")
    metadata = {
        "generated_at": datetime.now().isoformat(),
        "total_events": len(events),
        "countries": [
            {
                "name": country,
                "slug": slugify(country),
                "count": len(events_by_country[country])
            }
            for country in countries_sorted
            if country != "Unknown"
        ]
    }

    with open(OUTPUT_DIR / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    # Generate main calendar (all events)
    print("Generating calendar.ics (all events)...")
    all_cal = generate_ical(events, calendar_name="Smoothcomp Events")
    with open(OUTPUT_DIR / "calendar.ics", "wb") as f:
        f.write(all_cal)

    # Generate per-country calendars
    print(f"Generating {len(countries_sorted)} country calendars...")
    for country in countries_sorted:
        country_events = events_by_country[country]
        slug = slugify(country)
        cal_data = generate_ical(
            country_events,
            calendar_name=f"Smoothcomp {country} Events"
        )
        with open(OUTPUT_DIR / "calendars" / f"{slug}.ics", "wb") as f:
            f.write(cal_data)

    # Generate events.json (for potential future use)
    print("Generating events.json...")
    events_data = [e.to_dict() for e in events]
    with open(OUTPUT_DIR / "events.json", "w") as f:
        json.dump(events_data, f)

    print(f"\nDone! Files written to {OUTPUT_DIR}/")
    print(f"  - metadata.json")
    print(f"  - calendar.ics ({len(events)} events)")
    print(f"  - calendars/*.ics ({len(countries_sorted)} countries)")
    print(f"  - events.json")
    print(f"\nTo test locally:")
    print(f"  python -m http.server 8000 -d {OUTPUT_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate static Smoothcomp calendar files")
    parser.add_argument("--limit", type=int, help="Limit number of events (for testing)")
    args = parser.parse_args()

    asyncio.run(main(limit=args.limit))
