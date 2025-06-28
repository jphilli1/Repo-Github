import requests
from bs4 import BeautifulSoup
import re
import datetime
import pytz
from icalendar import Calendar, Event, Alarm
import os
import json
from pathlib import Path
import sys

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    print("Required Google API packages are not installed.")
    print("Please run the following commands in your Anaconda prompt:")
    print("conda install -c conda-forge google-api-python-client")
    print("conda install -c conda-forge google-auth-oauthlib")
    sys.exit(1)

# Google Calendar API setup
SCOPES = ['https://www.googleapis.com/auth/calendar']

class NYCParksMoviesScraper:
    """
    Scrapes NYC Parks website for Movies Under the Stars events and adds them to Google Calendar.
    Filters for Upper East Side locations.
    """
    
    def __init__(self, location_filter=None):
        """Initialize the scraper with location filter."""
        self.ny_timezone = pytz.timezone('America/New_York')
        self.base_url = 'https://www.nycgovparks.org'
        self.movies_url = f"{self.base_url}/events/movies-under-the-stars"
        self.location_filter = location_filter  # Can be a string or list of strings
        
        # Browser headers to avoid 403 error
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def is_location_match(self, location_text):
        """Check if the location matches the filter criteria."""
        if not self.location_filter:
            return True  # No filter, include all locations
        
        # Convert to list if it's a string
        filters = self.location_filter if isinstance(self.location_filter, list) else [self.location_filter]
        
        # Check if any filter matches
        location_lower = location_text.lower()
        for filter_term in filters:
            if filter_term.lower() in location_lower:
                return True
        
        return False
    
    def extract_date_from_section(self, section_text):
        """Extract date from section header like 'Wednesday, May 21, 2025'"""
        date_match = re.search(r'(\w+), (\w+) (\d+), (\d{4})', section_text)
        if date_match:
            weekday, month, day, year = date_match.groups()
            try:
                return datetime.datetime.strptime(f"{month} {day}, {year}", "%B %d, %Y").date()
            except ValueError:
                print(f"Failed to parse date: {month} {day}, {year}")
        return None
    
    def extract_time(self, time_text):
        """Extract start and end time from text like '7:30 p.m.–8:00 p.m.' or '8:00 p.m.'"""
        # Clean up time text for more reliable parsing
        cleaned_time = time_text.replace('.', '').strip()
        
        # Look for patterns like "7:30 p.m.–8:00 p.m." or "8:00 p.m."
        time_match = re.search(r'(\d+:\d+)\s*([ap]\.?m\.?)(?:\s*[–-]\s*(\d+:\d+)\s*([ap]\.?m\.?))?', cleaned_time, re.IGNORECASE)
        
        if not time_match:
            print(f"Could not parse time pattern from: '{time_text}'")
            return None, None
        
        # Process start time
        start_time_str = f"{time_match.group(1)} {time_match.group(2).replace('.', '')}"
        start_time_str = start_time_str.upper().replace('A.M', 'AM').replace('P.M', 'PM')
        
        try:
            start_time = datetime.datetime.strptime(start_time_str, "%I:%M %p").time()
        except ValueError:
            print(f"Error parsing start time: {start_time_str}")
            return None, None
        
        # Process end time if available
        if time_match.group(3):
            end_time_str = f"{time_match.group(3)} {time_match.group(4).replace('.', '')}"
            end_time_str = end_time_str.upper().replace('A.M', 'AM').replace('P.M', 'PM')
            
            try:
                end_time = datetime.datetime.strptime(end_time_str, "%I:%M %p").time()
            except ValueError:
                print(f"Error parsing end time: {end_time_str}")
                # Default to 2 hours after start time
                temp_dt = datetime.datetime.combine(datetime.date.today(), start_time) + datetime.timedelta(hours=2)
                end_time = temp_dt.time()
        else:
            # Default to 2 hours later if no end time
            temp_dt = datetime.datetime.combine(datetime.date.today(), start_time) + datetime.timedelta(hours=2)
            end_time = temp_dt.time()
        
        return start_time, end_time
    
    def scrape_movies(self):
        """Scrape the NYC Parks website for Movies Under the Stars events."""
        print(f"Scraping data from {self.movies_url}...")
        
        # Get the main page for Movies Under the Stars
        try:
            response = requests.get(self.movies_url, headers=self.headers)
            response.raise_for_status()
            print(f"Successfully retrieved page with status code: {response.status_code}")
        except requests.RequestException as e:
            print(f"Failed to retrieve data. Status code: {getattr(response, 'status_code', 'Unknown')}")
            print(f"Error details: {e}")
            return []
        
        # Save HTML to a file for debugging
        with open('debug_movie_page.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print("Saved HTML to debug_movie_page.html for inspection")
        
        # Use lxml parser instead of html.parser
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find all date headers
        day_headers = []
        for header in soup.find_all(['h2', 'h3']):
            if re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+\w+\s+\d+,\s+\d{4}', header.text):
                day_headers.append(header)
        
        print(f"Found {len(day_headers)} day headers")
        
        events = []
        
        # Process each day header
        for header in day_headers:
            event_date = self.extract_date_from_section(header.text)
            if not event_date:
                continue
            
            # Find all MAY events that are after the day header but before the next day header
            current_elem = header.next_element
            event_blocks = []
            
            # This will store all elements between this header and the next header
            elements_between_headers = []
            
            # Find the next day header
            next_header = None
            for next_h in header.find_next_siblings(['h2', 'h3']):
                if re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+\w+\s+\d+,\s+\d{4}', next_h.text):
                    next_header = next_h
                    break
            
            # If we found a next header, collect everything between
            if next_header:
                current = header.next_sibling
                while current and current != next_header:
                    elements_between_headers.append(current)
                    current = current.next_sibling
            else:
                # If no next header, just collect everything after
                current = header.next_sibling
                while current:
                    elements_between_headers.append(current)
                    current = current.next_sibling
            
            # Look for event blocks in the collected elements
            for elem in elements_between_headers:
                if hasattr(elem, 'name') and elem.name is not None:
                    # Check if this is an event block - look for MAY header followed by event content
                    movie_title = elem.find(string=lambda s: s and ('Movies Under the Stars' in s or 'Thelma' in s or 'Sonic' in s))
                    if movie_title:
                        event_blocks.append(elem)
            
            print(f"Found {len(event_blocks)} event blocks for {header.text}")
            
            # Process each event block
            for block in event_blocks:
                try:
                    # Extract movie title
                    title_elem = block.find(string=lambda s: s and ('Movies Under the Stars' in s or 'Thelma' in s or 'Sonic' in s))
                    title = title_elem.strip() if title_elem else "Unknown Movie"
                    
                    # Look for location - check all text inside the block
                    location = None
                    for text in block.find_all(text=True):
                        if 'at ' in text and any(term.lower() in text.lower() for term in [
                            'Park', 'Street', 'Avenue', 'Manhattan', 'Brooklyn', 'Entrance', 'Court'
                        ]):
                            location_match = re.search(r'at\s+(.+?)(?:,|$)', text)
                            if location_match:
                                potential_location = location_match.group(1).strip()
                                if self.is_location_match(potential_location):
                                    location = potential_location
                                    break
                    
                    # If no location found this way, check for any location pattern
                    if not location:
                        for text in block.find_all(text=True):
                            for term in self.location_filter:
                                if term.lower() in text.lower():
                                    location = text.strip()
                                    break
                            if location:
                                break
                    
                    # Check if this is in Manhattan or other borough
                    borough = None
                    for text in block.find_all(text=True):
                        borough_match = re.search(r'(Manhattan|Brooklyn|Queens|Bronx|Staten Island)', text)
                        if borough_match:
                            borough = borough_match.group(1)
                            break
                    
                    # If we found a borough but it's not part of the location, add it
                    if borough and location and borough not in location:
                        location = f"{location}, {borough}"
                    
                    # Skip this event if no location was found or if location doesn't match our filter
                    if not location or not self.is_location_match(location):
                        continue
                    
                    # Look for time information
                    time_text = None
                    for text in block.find_all(text=True):
                        if re.search(r'\d+:\d+\s*[ap]\.?m\.?', text):
                            time_text = text.strip()
                            break
                    
                    if not time_text:
                        print(f"Could not find time information for {title}")
                        continue
                    
                    # Parse the time
                    start_time, end_time = self.extract_time(time_text)
                    if not start_time:
                        continue
                    
                    # Create datetime objects
                    start_datetime = datetime.datetime.combine(event_date, start_time)
                    end_datetime = datetime.datetime.combine(event_date, end_time)
                    
                    # Make timezone aware
                    start_datetime = self.ny_timezone.localize(start_datetime)
                    end_datetime = self.ny_timezone.localize(end_datetime)
                    
                    # Look for a description - any text that's not title, location, or time
                    description = ""
                    for text in block.find_all(text=True):
                        text = text.strip()
                        if (text and 'Movies Under the Stars' not in text and 
                            time_text != text and 
                            (not location or location not in text) and
                            not re.search(r'^\s*$', text) and 
                            text != title):
                            if len(text) > 20:  # Avoid very short text
                                description += text + "\n"
                    
                    # Clean up description
                    description = description.strip()
                    if not description:
                        description = f"NYC Parks Movies Under the Stars event at {location}"
                    
                    # Check for an image to help identify the movie
                    img = block.find('img')
                    if img and 'src' in img.attrs:
                        img_url = img['src']
                        if not img_url.startswith('http'):
                            img_url = self.base_url + img_url
                        description += f"\n\nPoster image: {img_url}"
                    
                    # For debugging, add the block text to see what we're working with
                    block_text = ' '.join(block.get_text().split())
                    
                    # Create the event data
                    event_data = {
                        'summary': title,
                        'location': location,
                        'start_datetime': start_datetime,
                        'end_datetime': end_datetime,
                        'description': description,
                        'url': self.movies_url,
                        'debug_info': block_text
                    }
                    
                    events.append(event_data)
                    print(f"Added event: {title} at {location} on {event_date}")
                    
                except Exception as e:
                    print(f"Error processing event block: {e}")
        
        print(f"Total events found: {len(events)}")
        return events


class GoogleCalendarManager:
    """Manages Google Calendar operations."""
    
    def __init__(self):
        """Initialize Google Calendar API."""
        self.creds = None
        self.service = None
        self.initialize_service()
    
    def initialize_service(self):
        """Initialize the Google Calendar API service."""
        creds = None
        token_path = 'token.json'
        
        # Check if token.json exists with stored credentials
        if os.path.exists(token_path):
            try:
                creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            except Exception as e:
                print(f"Error reading token.json: {e}")
                if os.path.exists(token_path):
                    os.remove(token_path)
                creds = None
        
        # If no valid credentials, authenticate
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"Error refreshing credentials: {e}")
                    creds = None
            
            if not creds:
                try:
                    # Check for credentials.json
                    if not os.path.exists('credentials.json'):
                        print("credentials.json file not found! Please download it from Google Cloud Console.")
                        sys.exit(1)
                    
                    flow = InstalledAppFlow.from_client_secrets_file(
                        'credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0)
                    
                    # Save credentials for future use
                    with open(token_path, 'w') as token:
                        token.write(creds.to_json())
                        
                except Exception as e:
                    print(f"Error during authentication: {e}")
                    sys.exit(1)
        
        self.creds = creds
        
        # Create the Google Calendar API service
        try:
            self.service = build('calendar', 'v3', credentials=creds)
            print("Successfully connected to Google Calendar API")
        except HttpError as error:
            print(f"Error building calendar service: {error}")
            sys.exit(1)
    
    def add_event(self, event_data):
        """Add an event to Google Calendar."""
        # Create Google Calendar event format
        event = {
            'summary': event_data['summary'],
            'location': event_data['location'],
            'description': event_data['description'],
            'start': {
                'dateTime': event_data['start_datetime'].isoformat(),
                'timeZone': 'America/New_York',
            },
            'end': {
                'dateTime': event_data['end_datetime'].isoformat(),
                'timeZone': 'America/New_York',
            },
            'reminders': {
                'useDefault': False,
                'overrides': [
                    {'method': 'email', 'minutes': 24 * 60},  # Email notification 1 day before
                    {'method': 'popup', 'minutes': 24 * 60},  # Popup notification 1 day before
                ],
            },
        }
        
        # Add source URL if available
        if event_data.get('url'):
            event['source'] = {
                'url': event_data['url'],
                'title': 'Event Website'
            }
        
        try:
            event = self.service.events().insert(calendarId='primary', body=event).execute()
            print(f"Added to calendar: {event_data['summary']} ({event.get('htmlLink')})")
            return True
        except HttpError as error:
            print(f"An error occurred adding event to calendar: {error}")
            return False
    
    def add_events(self, events):
        """Add multiple events to Google Calendar."""
        success_count = 0
        for event_data in events:
            if self.add_event(event_data):
                success_count += 1
        
        print(f"Successfully added {success_count} of {len(events)} events to Google Calendar")
        return success_count


def main():
    """Main function to run the scraper and add events to Google Calendar."""
    
    print("Starting NYC Parks Movies Under the Stars scraper...")
    
    # Initialize Google Calendar manager
    calendar_manager = GoogleCalendarManager()
    
    # Scrape NYC Parks Movies Under the Stars with Upper East Side filter
    ues_locations = ['Upper East Side', 'Carl Schurz Park', 'East Harlem', 'Yorkville', 'UES',
                    'Entrance', 'Amelia Gorman Park', 'Manhattan', 'Dyker Beach Park']
    
    # Only scrape NYC Parks (no Carl Schurz website)
    nyc_parks_scraper = NYCParksMoviesScraper(location_filter=ues_locations)
    events = nyc_parks_scraper.scrape_movies()
    
    if not events:
        print("No events found to add to calendar.")
        return
    
    print(f"Found a total of {len(events)} events")
    
    # Print events details for debugging
    for i, event in enumerate(events):
        print(f"\nEvent {i+1}:")
        print(f"Title: {event['summary']}")
        print(f"Location: {event['location']}")
        print(f"Start: {event['start_datetime']}")
        print(f"End: {event['end_datetime']}")
        print(f"Description: {event['description'][:100]}...")
        
        # For debugging - print the block text
        if 'debug_info' in event:
            print(f"Block text: {event['debug_info'][:100]}...")
    
    # Ask user if they want to add these events to calendar
    confirm = input("\nDo you want to add these events to your Google Calendar? (y/n): ")
    if confirm.lower() != 'y':
        print("Events were not added to calendar.")
        return
    
    # Add events to Google Calendar
    calendar_manager.add_events(events)
    
    print("\nDone! Upper East Side outdoor movie events have been added to your Google Calendar.")


if __name__ == "__main__":
    main()