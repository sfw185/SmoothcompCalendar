"""
Smoothcomp Calendar Web Service

FastAPI server providing dynamic webcal feeds with filtering.
Fully automatic - no manual maintenance required.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import Response, HTMLResponse

from scraper import SmoothcompScraper
from cache import EventCache
from calendar_gen import generate_ical


# Configuration
AUTO_REFRESH_MINUTES = int(os.environ.get("AUTO_REFRESH_MINUTES", "60"))

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
    event_count = 0

    try:
        print(f"[{refresh_time}] Starting incremental cache refresh...")
        async with SmoothcompScraper(rate_limit=0.3) as scraper:
            async for event, current, total in scraper.scrape_events_iter():
                await cache.upsert_event(event, refresh_time)
                event_count += 1
                if current % 50 == 0 or current == total:
                    print(f"  Progress: {current}/{total} ({event_count} stored)")

        # Cleanup events no longer in source
        await cache.cleanup_stale_events(refresh_time)
        await cache.mark_refresh_complete()
        print(f"[{datetime.now()}] Cache refresh complete: {event_count} events")
    except Exception as e:
        print(f"[{datetime.now()}] Cache refresh failed: {e}")
        # Don't cleanup on failure - keep existing data
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


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page with usage instructions."""
    base_url = str(request.base_url).rstrip('/')
    countries = await cache.get_countries()
    event_count = await cache.get_event_count()
    last_update = await cache.get_last_update()

    countries_list = '\n'.join(f'        <li><code>{c}</code></li>' for c in countries[:20])
    if len(countries) > 20:
        countries_list += f'\n        <li>... and {len(countries) - 20} more</li>'

    status_text = "Scraping in progress..." if _scraping else f"{event_count} events"
    update_text = last_update.strftime('%Y-%m-%d %H:%M UTC') if last_update else 'Never'

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Smoothcomp Calendar</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                   max-width: 800px; margin: 50px auto; padding: 20px; }}
            code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 3px; }}
            pre {{ background: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; }}
            .endpoint {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
            h3 {{ margin-top: 0; }}
        </style>
    </head>
    <body>
        <h1>Smoothcomp Calendar</h1>
        <p>Dynamic webcal feeds for Smoothcomp martial arts events.</p>

        <p><strong>Status:</strong> {status_text} (Last update: {update_text})</p>

        <h2>Subscribe to Calendar</h2>

        <div class="endpoint">
            <h3>GET /calendar.ics</h3>
            <p>Subscribe to this URL in your calendar app. Supports filters:</p>
            <ul>
                <li><code>country</code> - Filter by country (e.g., "Australia", "United States")</li>
                <li><code>sport</code> - Filter by sport type (e.g., "bjj", "grappling")</li>
                <li><code>days</code> - Only events in the next N days</li>
            </ul>
            <p><strong>Examples:</strong></p>
            <pre>
# All upcoming events
{base_url}/calendar.ics

# Australian events only
{base_url}/calendar.ics?country=Australia

# US BJJ events in the next 30 days
{base_url}/calendar.ics?country=United%20States&sport=bjj&days=30

# Add to calendar app (replace http with webcal)
webcal://{request.url.netloc}/calendar.ics?country=Australia
            </pre>
        </div>

        <h2>API Endpoints</h2>

        <div class="endpoint">
            <h3>GET /events</h3>
            <p>Get events as JSON. Same filters as /calendar.ics</p>
        </div>

        <div class="endpoint">
            <h3>GET /countries</h3>
            <p>List all available countries.</p>
        </div>

        <div class="endpoint">
            <h3>GET /status</h3>
            <p>Cache status and health check.</p>
        </div>

        <h2>Available Countries</h2>
        <ul>
{countries_list}
        </ul>
    </body>
    </html>
    """


@app.get("/calendar.ics")
async def get_calendar(
    country: Optional[str] = Query(None, description="Filter by country"),
    sport: Optional[str] = Query(None, description="Filter by sport"),
    days: Optional[int] = Query(None, description="Events in next N days"),
    limit: Optional[int] = Query(None, description="Max events to return")
):
    """Get events as an iCalendar feed."""
    # Auto-refresh if stale
    await maybe_refresh_cache()

    start_before = None
    if days:
        start_before = datetime.now() + timedelta(days=days)

    events = await cache.get_events(
        country=country,
        sport=sport,
        start_before=start_before,
        limit=limit
    )

    # Generate calendar name based on filters
    name_parts = ["Smoothcomp"]
    if country:
        name_parts.append(country)
    if sport:
        name_parts.append(sport.upper())
    name_parts.append("Events")

    ical_data = generate_ical(events, calendar_name=' '.join(name_parts))

    return Response(
        content=ical_data,
        media_type="text/calendar",
        headers={
            "Content-Disposition": "attachment; filename=smoothcomp.ics"
        }
    )


@app.get("/events")
async def get_events_json(
    country: Optional[str] = Query(None, description="Filter by country"),
    sport: Optional[str] = Query(None, description="Filter by sport"),
    days: Optional[int] = Query(None, description="Events in next N days"),
    limit: Optional[int] = Query(None, description="Max events to return")
):
    """Get events as JSON."""
    await maybe_refresh_cache()

    start_before = None
    if days:
        start_before = datetime.now() + timedelta(days=days)

    events = await cache.get_events(
        country=country,
        sport=sport,
        start_before=start_before,
        limit=limit
    )

    return {
        "count": len(events),
        "filters": {
            "country": country,
            "sport": sport,
            "days": days
        },
        "events": [e.to_dict() for e in events]
    }


@app.get("/countries")
async def get_countries():
    """Get list of available countries."""
    return {"countries": await cache.get_countries()}


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


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
