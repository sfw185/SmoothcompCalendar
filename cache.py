"""
Event Cache - PostgreSQL Backend

Async PostgreSQL cache for scraped events with incremental updates.
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import asyncpg

from scraper import Event


class EventCache:
    """PostgreSQL-based event cache with incremental updates."""

    def __init__(self, database_url: Optional[str] = None, ttl_hours: int = 1):
        """
        Initialize the cache.

        Args:
            database_url: PostgreSQL connection URL (defaults to DATABASE_URL env var)
            ttl_hours: Cache time-to-live in hours (triggers refresh when exceeded)
        """
        self.database_url = database_url or os.environ.get("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable required")

        self.ttl = timedelta(hours=ttl_hours)
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Initialize connection pool and create schema."""
        self._pool = await asyncpg.create_pool(
            self.database_url,
            min_size=2,
            max_size=10
        )
        await self._init_schema()

    async def close(self):
        """Close connection pool."""
        if self._pool:
            await self._pool.close()

    async def _init_schema(self):
        """Initialize database schema with migrations."""
        async with self._pool.acquire() as conn:
            # Check if events table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'events'
                )
            """)

            if table_exists:
                # Migration: rename cached_at to updated_at if needed
                has_cached_at = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_name = 'events' AND column_name = 'cached_at'
                    )
                """)
                if has_cached_at:
                    await conn.execute("ALTER TABLE events RENAME COLUMN cached_at TO updated_at")

                # Ensure updated_at column exists
                has_updated_at = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_name = 'events' AND column_name = 'updated_at'
                    )
                """)
                if not has_updated_at:
                    await conn.execute("ALTER TABLE events ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW()")
            else:
                # Create fresh table
                await conn.execute("""
                    CREATE TABLE events (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        url TEXT NOT NULL,
                        start_date TIMESTAMPTZ,
                        end_date TIMESTAMPTZ,
                        location TEXT,
                        city TEXT,
                        country TEXT,
                        sport TEXT,
                        organizer TEXT,
                        participants INTEGER,
                        registration_open BOOLEAN DEFAULT TRUE,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)

            # Create indexes (safe to run multiple times)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_country ON events(country)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_start_date ON events(start_date)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_updated_at ON events(updated_at)
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

    async def get_last_update(self) -> Optional[datetime]:
        """Get the timestamp of last successful refresh completion."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT value FROM cache_meta WHERE key = 'last_update'"
            )
            if row:
                return datetime.fromisoformat(row['value'])
            return None

    async def is_stale(self) -> bool:
        """Check if cache is stale (older than TTL)."""
        last_update = await self.get_last_update()
        if not last_update:
            return True
        return datetime.now() - last_update > self.ttl

    async def upsert_event(self, event: Event, refresh_time: datetime):
        """
        Insert or update a single event.

        Args:
            event: Event to upsert
            refresh_time: Timestamp of this refresh cycle (for cleanup tracking)
        """
        # Skip past events
        if event.start_date:
            event_start = event.start_date.replace(tzinfo=None) if event.start_date.tzinfo else event.start_date
            if event_start < datetime.now():
                return

        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO events
                (id, name, url, start_date, end_date, location, city,
                 country, sport, organizer, participants, registration_open, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    url = EXCLUDED.url,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    location = EXCLUDED.location,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    sport = EXCLUDED.sport,
                    organizer = EXCLUDED.organizer,
                    participants = EXCLUDED.participants,
                    registration_open = EXCLUDED.registration_open,
                    updated_at = EXCLUDED.updated_at
            """,
                event.id,
                event.name,
                event.url,
                event.start_date,
                event.end_date,
                event.location,
                event.city,
                event.country,
                event.sport,
                event.organizer,
                event.participants,
                event.registration_open,
                refresh_time
            )

    async def cleanup_stale_events(self, refresh_time: datetime):
        """
        Remove events not updated in this refresh cycle and past events.
        Call this after a successful refresh completes.

        Args:
            refresh_time: Timestamp of the refresh cycle that just completed
        """
        async with self._pool.acquire() as conn:
            # Delete events not updated in this refresh (no longer on source)
            deleted_old = await conn.execute(
                "DELETE FROM events WHERE updated_at < $1",
                refresh_time
            )
            # Delete past events
            deleted_past = await conn.execute(
                "DELETE FROM events WHERE start_date < NOW()"
            )
            return deleted_old, deleted_past

    async def mark_refresh_complete(self):
        """Mark that a refresh cycle completed successfully."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO cache_meta (key, value)
                VALUES ('last_update', $1)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, datetime.now().isoformat())

    async def get_events(
        self,
        country: Optional[str] = None,
        sport: Optional[str] = None,
        start_after: Optional[datetime] = None,
        start_before: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> list[Event]:
        """
        Get upcoming events from cache with optional filters.
        Automatically excludes past events.
        """
        query = "SELECT * FROM events WHERE start_date >= NOW()"
        params = []
        param_count = 0

        if country:
            param_count += 1
            query += f" AND LOWER(country) LIKE ${param_count}"
            params.append(f"%{country.lower()}%")

        if sport:
            param_count += 1
            query += f" AND LOWER(sport) LIKE ${param_count}"
            params.append(f"%{sport.lower()}%")

        if start_after:
            param_count += 1
            query += f" AND start_date >= ${param_count}"
            params.append(start_after)

        if start_before:
            param_count += 1
            query += f" AND start_date <= ${param_count}"
            params.append(start_before)

        query += " ORDER BY start_date ASC"

        if limit:
            param_count += 1
            query += f" LIMIT ${param_count}"
            params.append(limit)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [
            Event(
                id=row['id'],
                name=row['name'],
                url=row['url'],
                start_date=row['start_date'],
                end_date=row['end_date'],
                location=row['location'],
                city=row['city'],
                country=row['country'],
                sport=row['sport'],
                organizer=row['organizer'],
                participants=row['participants'],
                registration_open=row['registration_open']
            )
            for row in rows
        ]

    async def get_countries(self) -> list[str]:
        """Get list of unique countries with upcoming events."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT country FROM events
                WHERE country IS NOT NULL
                  AND country != ''
                  AND start_date >= NOW()
                ORDER BY country
            """)
            return [row['country'] for row in rows]

    async def get_event_count(self) -> int:
        """Get total number of upcoming events in cache."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) as count FROM events WHERE start_date >= NOW()"
            )
            return row['count']
