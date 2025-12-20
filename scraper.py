"""
Smoothcomp Event Scraper

Fetches upcoming events from smoothcomp.com and extracts event details.
"""

import asyncio
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup


@dataclass
class Event:
    """Represents a Smoothcomp event."""
    id: str
    name: str
    url: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    location: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    sport: Optional[str] = None
    organizer: Optional[str] = None
    participants: Optional[int] = None
    registration_open: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary with ISO date strings."""
        d = asdict(self)
        if self.start_date:
            d['start_date'] = self.start_date.isoformat()
        if self.end_date:
            d['end_date'] = self.end_date.isoformat()
        return d


class SmoothcompScraper:
    """Scrapes events from Smoothcomp."""

    BASE_URL = "https://smoothcomp.com"
    EVENTS_URL = "https://smoothcomp.com/en/events/upcoming"

    def __init__(self, rate_limit: float = 0.5):
        """
        Initialize scraper.

        Args:
            rate_limit: Seconds to wait between requests (default 0.5s)
        """
        self.rate_limit = rate_limit
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            headers={"User-Agent": "SmoothcompCalendar/1.0"}
        )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def get_event_urls(self) -> list[str]:
        """
        Fetch the main events page and extract all event URLs from JSON-LD.

        Returns:
            List of event URLs
        """
        async with self._session.get(self.EVENTS_URL) as resp:
            html = await resp.text()

        soup = BeautifulSoup(html, 'html.parser')

        # Find JSON-LD script tag
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if data.get('@type') == 'ItemList':
                    return [
                        item['url']
                        for item in data.get('itemListElement', [])
                        if 'url' in item
                    ]
            except (json.JSONDecodeError, TypeError):
                continue

        return []

    async def get_event_details(self, url: str) -> Optional[Event]:
        """
        Fetch an individual event page and extract details.

        Args:
            url: Event page URL

        Returns:
            Event object or None if parsing fails
        """
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

        soup = BeautifulSoup(html, 'html.parser')

        # Extract event ID from URL
        event_id_match = re.search(r'/event/(\d+)', url)
        event_id = event_id_match.group(1) if event_id_match else url

        # Try to get data from JSON-LD first (most reliable)
        event = self._parse_jsonld(soup, url, event_id)
        if event:
            return event

        # Fallback to HTML parsing
        return self._parse_html(soup, url, event_id)

    def _parse_jsonld(self, soup: BeautifulSoup, url: str, event_id: str) -> Optional[Event]:
        """Parse event data from JSON-LD schema."""
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if data.get('@type') == 'SportsEvent':
                    return self._event_from_jsonld(data, url, event_id)
            except (json.JSONDecodeError, TypeError):
                continue
        return None

    def _event_from_jsonld(self, data: dict, url: str, event_id: str) -> Event:
        """Create Event from JSON-LD data."""
        location_data = data.get('location', {})
        address_data = location_data.get('address', {})

        start_date = None
        end_date = None

        if data.get('startDate'):
            try:
                start_date = datetime.fromisoformat(data['startDate'].replace('Z', '+00:00'))
            except ValueError:
                pass

        if data.get('endDate'):
            try:
                end_date = datetime.fromisoformat(data['endDate'].replace('Z', '+00:00'))
            except ValueError:
                pass

        # Extract country
        country = address_data.get('addressCountry', '')
        if isinstance(country, dict):
            country = country.get('name', '')

        return Event(
            id=event_id,
            name=data.get('name', 'Unknown Event'),
            url=url,
            start_date=start_date,
            end_date=end_date,
            location=location_data.get('name', ''),
            city=address_data.get('addressLocality', ''),
            country=country,
            sport=data.get('sport', 'Grappling'),
            organizer=data.get('organizer', {}).get('name', ''),
            registration_open=True  # Default, could parse from page
        )

    def _parse_html(self, soup: BeautifulSoup, url: str, event_id: str) -> Optional[Event]:
        """Fallback HTML parsing when JSON-LD is not available."""
        title = soup.find('title')
        name = title.text.strip() if title else 'Unknown Event'

        # Clean up title (often includes "| Smoothcomp")
        name = re.sub(r'\s*\|\s*Smoothcomp.*$', '', name)

        return Event(
            id=event_id,
            name=name,
            url=url
        )

    async def scrape_events_iter(
        self,
        max_events: Optional[int] = None,
        existing_ids: Optional[set[str]] = None
    ):
        """
        Async generator that yields events one at a time as they're scraped.
        Prioritizes new events (not in existing_ids) first.

        Args:
            max_events: Maximum number of events to fetch (None for all)
            existing_ids: Set of event IDs already in database (scraped last)

        Yields:
            Tuple of (event, current_index, total_count, is_new)
        """
        urls = await self.get_event_urls()

        if max_events:
            urls = urls[:max_events]

        # Extract IDs and split into new vs existing
        if existing_ids:
            new_urls = []
            update_urls = []
            for url in urls:
                event_id_match = re.search(r'/event/(\d+)', url)
                event_id = event_id_match.group(1) if event_id_match else None
                if event_id and event_id in existing_ids:
                    update_urls.append(url)
                else:
                    new_urls.append(url)
            # Process new events first, then updates
            ordered_urls = new_urls + update_urls
            new_count = len(new_urls)
        else:
            ordered_urls = urls
            new_count = len(urls)

        total = len(ordered_urls)

        for i, url in enumerate(ordered_urls):
            event = await self.get_event_details(url)
            if event:
                is_new = i < new_count
                yield event, i + 1, total, is_new

            # Rate limiting
            await asyncio.sleep(self.rate_limit)

    async def scrape_events(
        self,
        max_events: Optional[int] = None,
        progress_callback=None,
        existing_ids: Optional[set[str]] = None
    ) -> list[Event]:
        """
        Scrape all upcoming events (batch mode).

        Args:
            max_events: Maximum number of events to fetch (None for all)
            progress_callback: Optional callback(current, total) for progress
            existing_ids: Set of event IDs already in database

        Returns:
            List of Event objects
        """
        events = []
        async for event, current, total, is_new in self.scrape_events_iter(max_events, existing_ids):
            events.append(event)
            if progress_callback:
                progress_callback(current, total)
        return events


async def main():
    """Test the scraper."""
    print("Starting Smoothcomp scraper...")

    async with SmoothcompScraper(rate_limit=0.3) as scraper:
        # First, get all event URLs
        print("Fetching event URLs...")
        urls = await scraper.get_event_urls()
        print(f"Found {len(urls)} event URLs")

        # Scrape first 5 events as a test
        print("\nScraping first 5 events for testing...")
        events = await scraper.scrape_events(
            max_events=5,
            progress_callback=lambda c, t: print(f"  Progress: {c}/{t}")
        )

        print(f"\nScraped {len(events)} events:")
        for event in events:
            print(f"\n  {event.name}")
            print(f"    Date: {event.start_date}")
            print(f"    Location: {event.city}, {event.country}")
            print(f"    URL: {event.url}")


if __name__ == "__main__":
    asyncio.run(main())
