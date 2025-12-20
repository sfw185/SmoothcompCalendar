"""
Smoothcomp Calendar Web Service

FastAPI server providing dynamic webcal feeds with filtering.
Fully automatic - no manual maintenance required.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
from urllib.parse import quote

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


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page with URL generator."""
    base_url = str(request.base_url).rstrip('/')
    host = request.url.netloc
    countries = await cache.get_countries()
    sports = await cache.get_sports()
    event_count = await cache.get_event_count()
    last_update = await cache.get_last_update()

    status_text = "Scraping in progress..." if _scraping else f"{event_count} events"
    update_text = last_update.strftime('%Y-%m-%d %H:%M UTC') if last_update else 'Never'

    # Build country options
    country_options = '<option value="">All Countries</option>\n'
    for c in countries:
        country_options += f'        <option value="{c["country"]}">{c["country"]} ({c["count"]})</option>\n'

    # Build sport options
    sport_options = '<option value="">All Sports</option>\n'
    for s in sports:
        sport_options += f'        <option value="{s["sport"]}">{s["sport"]} ({s["count"]})</option>\n'

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Smoothcomp Calendar</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            * {{ box-sizing: border-box; }}
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                max-width: 700px;
                margin: 0 auto;
                padding: 20px;
                background: #f5f5f5;
            }}
            h1 {{ color: #333; margin-bottom: 5px; }}
            .status {{ color: #666; font-size: 14px; margin-bottom: 30px; }}
            .card {{
                background: white;
                border-radius: 10px;
                padding: 25px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }}
            label {{
                display: block;
                margin-bottom: 8px;
                font-weight: 600;
                color: #333;
            }}
            select {{
                width: 100%;
                padding: 12px;
                font-size: 16px;
                border: 2px solid #ddd;
                border-radius: 8px;
                margin-bottom: 20px;
                background: white;
            }}
            select:focus {{
                outline: none;
                border-color: #007AFF;
            }}
            .url-box {{
                background: #f8f8f8;
                border: 2px solid #ddd;
                border-radius: 8px;
                padding: 15px;
                font-family: monospace;
                font-size: 13px;
                word-break: break-all;
                margin-bottom: 15px;
            }}
            .buttons {{
                display: flex;
                gap: 10px;
            }}
            button {{
                flex: 1;
                padding: 14px 20px;
                font-size: 16px;
                font-weight: 600;
                border: none;
                border-radius: 8px;
                cursor: pointer;
                transition: transform 0.1s, box-shadow 0.1s;
            }}
            button:active {{
                transform: scale(0.98);
            }}
            .btn-primary {{
                background: #007AFF;
                color: white;
            }}
            .btn-secondary {{
                background: #e0e0e0;
                color: #333;
            }}
            .btn-primary:hover {{ background: #0056b3; }}
            .btn-secondary:hover {{ background: #d0d0d0; }}
            .info {{
                font-size: 13px;
                color: #666;
                margin-top: 15px;
                line-height: 1.5;
            }}
            .api-links {{
                display: flex;
                gap: 15px;
                flex-wrap: wrap;
            }}
            .instructions h3 {{
                font-size: 15px;
                margin: 20px 0 10px 0;
                color: #333;
            }}
            .instructions h3:first-child {{
                margin-top: 10px;
            }}
            .instructions ol {{
                margin: 0;
                padding-left: 20px;
                color: #555;
                line-height: 1.7;
            }}
            .instructions a {{
                color: #007AFF;
            }}
        </style>
    </head>
    <body>
        <h1>Smoothcomp Calendar</h1>
        <p class="status">{status_text} &bull; Last update: {update_text}</p>

        <div class="card">
            <label for="country">Country</label>
            <select id="country" onchange="updateUrl()">
                {country_options}
            </select>

            <label for="sport">Sport</label>
            <select id="sport" onchange="updateUrl()">
                {sport_options}
            </select>

            <label>Your Calendar URL</label>
            <div class="url-box" id="urlBox">{base_url}/calendar.ics</div>

            <button class="btn-primary" onclick="copyUrl()" style="width: 100%;">Copy URL</button>
        </div>

        <div class="card">
            <label>How to Add to Your Calendar</label>

            <div class="instructions">
                <h3>Apple Calendar (Mac/iPhone)</h3>
                <ol>
                    <li>Copy the URL above</li>
                    <li>Open Calendar app</li>
                    <li>File &rarr; New Calendar Subscription (Mac) or<br>
                        Settings &rarr; Calendar &rarr; Accounts &rarr; Add Account &rarr; Other &rarr; Add Subscribed Calendar (iPhone)</li>
                    <li>Paste the URL and click Subscribe</li>
                </ol>

                <h3>Google Calendar</h3>
                <ol>
                    <li>Copy the URL above</li>
                    <li>Open <a href="https://calendar.google.com" target="_blank">Google Calendar</a></li>
                    <li>Click the + next to "Other calendars"</li>
                    <li>Select "From URL"</li>
                    <li>Paste the URL and click "Add calendar"</li>
                </ol>
            </div>
        </div>

        <script>
            function updateUrl() {{
                const country = document.getElementById('country').value;
                const sport = document.getElementById('sport').value;

                let url = '{base_url}/calendar.ics';
                const params = [];

                if (country) params.push('country=' + encodeURIComponent(country));
                if (sport) params.push('sport=' + encodeURIComponent(sport));

                if (params.length > 0) url += '?' + params.join('&');

                document.getElementById('urlBox').textContent = url;
            }}

            function copyUrl() {{
                const url = document.getElementById('urlBox').textContent;
                navigator.clipboard.writeText(url).then(() => {{
                    const btn = event.target;
                    btn.textContent = 'Copied!';
                    setTimeout(() => btn.textContent = 'Copy URL', 2000);
                }});
            }}
        </script>
    </body>
    </html>
    """


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
        "filters": {
            "country": country,
            "sport": sport
        },
        "events": [e.to_dict() for e in events]
    }


@app.get("/countries")
async def get_countries():
    """Get list of countries with event counts."""
    countries = await cache.get_countries()
    return {
        "total": len(countries),
        "countries": countries
    }


@app.get("/sports")
async def get_sports():
    """Get list of sports with event counts."""
    sports = await cache.get_sports()
    return {
        "total": len(sports),
        "sports": sports
    }


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
