"""
iCal/Webcal Generator

Converts events to iCalendar format for webcal subscriptions.
"""

from datetime import datetime, timedelta
from typing import Optional

from icalendar import Calendar, Event as ICalEvent, vText

from scraper import Event


def generate_ical(
    events: list[Event],
    calendar_name: str = "Smoothcomp Events",
    ttl_minutes: int = 360
) -> bytes:
    """
    Generate an iCalendar feed from events.

    Args:
        events: List of Event objects
        calendar_name: Name for the calendar
        ttl_minutes: Refresh interval suggestion in minutes

    Returns:
        iCalendar data as bytes (UTF-8 encoded)
    """
    cal = Calendar()

    # Calendar metadata
    cal.add('prodid', '-//Smoothcomp Calendar//smoothcomp.com//')
    cal.add('version', '2.0')
    cal.add('calscale', 'GREGORIAN')
    cal.add('method', 'PUBLISH')
    cal.add('x-wr-calname', calendar_name)
    cal.add('x-wr-timezone', 'UTC')

    # Refresh interval (for clients that support it)
    cal.add('refresh-interval;value=duration', f'PT{ttl_minutes}M')
    cal.add('x-published-ttl', f'PT{ttl_minutes}M')

    for event in events:
        ical_event = create_ical_event(event)
        if ical_event:
            cal.add_component(ical_event)

    return cal.to_ical()


def create_ical_event(event: Event) -> Optional[ICalEvent]:
    """
    Create an iCalendar event from a Smoothcomp Event.

    Args:
        event: Smoothcomp Event object

    Returns:
        iCalendar Event or None if required fields are missing
    """
    if not event.start_date:
        return None

    ical_event = ICalEvent()

    # Required fields
    ical_event.add('uid', f'{event.id}@smoothcomp.com')
    ical_event.add('dtstamp', datetime.now())
    ical_event.add('dtstart', event.start_date)

    # End date (default to start + 8 hours if not specified)
    if event.end_date:
        ical_event.add('dtend', event.end_date)
    else:
        ical_event.add('dtend', event.start_date + timedelta(hours=8))

    # Summary (event name)
    ical_event.add('summary', event.name)

    # Location
    location_parts = []
    if event.location:
        location_parts.append(event.location)
    if event.city:
        location_parts.append(event.city)
    if event.country:
        location_parts.append(event.country)

    if location_parts:
        ical_event.add('location', ', '.join(location_parts))

    # Description
    description_lines = []
    if event.sport:
        description_lines.append(f"Sport: {event.sport}")
    if event.organizer:
        description_lines.append(f"Organizer: {event.organizer}")
    if event.participants:
        description_lines.append(f"Participants: {event.participants}")
    description_lines.append(f"Details: {event.url}")

    ical_event.add('description', '\n'.join(description_lines))

    # URL
    ical_event.add('url', event.url)

    # Categories
    if event.sport:
        ical_event.add('categories', [event.sport])

    return ical_event


def generate_webcal_url(
    base_url: str,
    country: Optional[str] = None,
    sport: Optional[str] = None,
    days: Optional[int] = None
) -> str:
    """
    Generate a webcal:// subscription URL.

    Args:
        base_url: Base HTTP URL of the calendar endpoint
        country: Optional country filter
        sport: Optional sport filter
        days: Optional number of days to include

    Returns:
        webcal:// URL for calendar subscription
    """
    # Convert http(s) to webcal
    webcal_url = base_url.replace('https://', 'webcal://').replace('http://', 'webcal://')

    params = []
    if country:
        params.append(f'country={country}')
    if sport:
        params.append(f'sport={sport}')
    if days:
        params.append(f'days={days}')

    if params:
        webcal_url += '?' + '&'.join(params)

    return webcal_url
