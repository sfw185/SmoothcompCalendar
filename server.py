"""
Smoothcomp Calendar Web Service

FastAPI server providing dynamic webcal feeds with filtering.
Fully automatic - no manual maintenance required.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import Response, FileResponse

from scraper import SmoothcompScraper
from cache import EventCache
from calendar_gen import generate_ical


# Configuration
AUTO_REFRESH_MINUTES = int(os.environ.get("AUTO_REFRESH_MINUTES", "60"))
TEMPLATES_DIR = Path(__file__).parent / "templates"

# Global state
cache: Optional[EventCache] = None
_scrape_lock = asyncio.Lock()
_scraping = False


async def refresh_cache():
    """Background task to refresh the event cache incrementally."""
    global _scraping

    async with _scrape_lock:
        if _scraping:
            return
        _scraping = True

    refresh_time = datetime.now()
    new_count = 0
    update_count = 0

    try:
        # Get existing event IDs to prioritize new events
        existing_ids = await cache.get_existing_event_ids()
        print(f"[{refresh_time}] Starting refresh ({len(existing_ids)} existing events, new events first)...")

        async with SmoothcompScraper(rate_limit=0.3) as scraper:
            async for event, current, total, is_new in scraper.scrape_events_iter(existing_ids=existing_ids):
                await cache.upsert_event(event, refresh_time)
                if is_new:
                    new_count += 1
                else:
                    update_count += 1

                if current % 50 == 0 or current == total:
                    print(f"  Progress: {current}/{total} (new: {new_count}, updated: {update_count})")

        # Cleanup events no longer in source
        await cache.cleanup_stale_events(refresh_time)
        await cache.mark_refresh_complete()
        print(f"[{datetime.now()}] Refresh complete: {new_count} new, {update_count} updated")
    except Exception as e:
        print(f"[{datetime.now()}] Refresh failed: {e} (keeping {new_count} new events)")
        # Don't cleanup on failure - keep existing + any new data
    finally:
        _scraping = False


async def maybe_refresh_cache():
    """Trigger background refresh if cache is stale."""
    if _scraping:
        return

    is_stale = await cache.is_stale()
    if is_stale:
        asyncio.create_task(refresh_cache())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown - connect to DB and start initial scrape if needed."""
    global cache

    # Initialize database connection
    cache = EventCache(ttl_hours=1)
    await cache.connect()

    # Check if cache needs population
    is_stale = await cache.is_stale()
    event_count = await cache.get_event_count()

    if is_stale or event_count == 0:
        asyncio.create_task(refresh_cache())

    yield

    # Cleanup
    await cache.close()


app = FastAPI(
    title="Smoothcomp Calendar",
    description="Dynamic webcal feeds for Smoothcomp events",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================================================
# Pages
# ============================================================================

@app.get("/")
async def home():
    """Serve the home page."""
    return FileResponse(TEMPLATES_DIR / "index.html")


# ============================================================================
# Calendar Endpoints
# ============================================================================

@app.get("/calendar.ics")
async def get_calendar(
    country: Optional[str] = Query(None, description="Filter by country"),
    sport: Optional[str] = Query(None, description="Filter by sport"),
    limit: Optional[int] = Query(None, description="Max events to return")
):
    """Get events as an iCalendar feed."""
    await maybe_refresh_cache()

    events = await cache.get_events(
        country=country,
        sport=sport,
        limit=limit
    )

    # Generate calendar name based on filters
    name_parts = ["Smoothcomp"]
    if country:
        name_parts.append(country)
    if sport:
        name_parts.append(sport)
    name_parts.append("Events")

    ical_data = generate_ical(events, calendar_name=' '.join(name_parts))

    return Response(
        content=ical_data,
        media_type="text/calendar",
        headers={
            "Content-Disposition": "attachment; filename=smoothcomp.ics"
        }
    )


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/events")
async def get_events_json(
    country: Optional[str] = Query(None, description="Filter by country"),
    sport: Optional[str] = Query(None, description="Filter by sport"),
    limit: Optional[int] = Query(None, description="Max events to return")
):
    """Get events as JSON."""
    await maybe_refresh_cache()

    events = await cache.get_events(
        country=country,
        sport=sport,
        limit=limit
    )

    return {
        "count": len(events),
        "filters": {"country": country, "sport": sport},
        "events": [e.to_dict() for e in events]
    }


@app.get("/countries")
async def get_countries():
    """Get list of countries with event counts."""
    return {
        "total": len(countries := await cache.get_countries()),
        "countries": countries
    }


@app.get("/sports")
async def get_sports():
    """Get list of sports with event counts."""
    return {
        "total": len(sports := await cache.get_sports()),
        "sports": sports
    }


@app.get("/filter-options")
async def get_filter_options(
    country: Optional[str] = Query(None, description="Current country filter"),
    sport: Optional[str] = Query(None, description="Current sport filter")
):
    """Get filtered dropdown options based on current selection."""
    return await cache.get_filter_options(country=country, sport=sport)


@app.get("/status")
async def get_status():
    """Get cache status - also serves as health check."""
    last_update = await cache.get_last_update()
    age_minutes = None
    if last_update:
        age_minutes = int((datetime.now() - last_update).total_seconds() / 60)

    return {
        "healthy": True,
        "event_count": await cache.get_event_count(),
        "last_update": last_update.isoformat() if last_update else None,
        "cache_age_minutes": age_minutes,
        "auto_refresh_threshold_minutes": AUTO_REFRESH_MINUTES,
        "scraping_in_progress": _scraping
    }


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
