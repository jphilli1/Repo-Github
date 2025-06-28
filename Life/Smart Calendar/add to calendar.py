import datetime
import os.path
import sys
import json
from pathlib import Path

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

class GoogleServicesConnector:
    """A connector class for Google APIs that can be extended for different services."""
    
    def __init__(self, credentials_path='credentials.json'):
        """Initialize the connector with the path to credentials file."""
        self.credentials_path = credentials_path
        self.services = {}
        self.creds = None
    
    def authenticate(self, scopes):
        """Authenticate with Google using OAuth."""
        creds = None
        token_path = 'token.json'
        
        # Check if token.json exists with stored credentials
        if os.path.exists(token_path):
            try:
                creds = Credentials.from_authorized_user_file(token_path, scopes)
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
                    if not os.path.exists(self.credentials_path):
                        self._create_credentials_file()
                    
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, scopes)
                    creds = flow.run_local_server(port=0)
                    
                    # Save credentials for future use
                    with open(token_path, 'w') as token:
                        token.write(creds.to_json())
                        
                except Exception as e:
                    print(f"Error during authentication: {e}")
                    return None
        
        self.creds = creds
        return creds
    
    def _create_credentials_file(self):
        """Helper to create a credentials.json file if it doesn't exist."""
        print("credentials.json file not found!")
        print("Let's create one manually...")
        
        client_id = input("Enter your client ID: ")
        client_secret = input("Enter your client secret: ")
        project_id = input("Enter your project ID (or press Enter to skip): ") or "unknown-project"
        
        credentials = {
            "installed": {
                "client_id": client_id,
                "project_id": project_id,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_secret": client_secret,
                "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob"]
            }
        }
        
        with open(self.credentials_path, 'w') as f:
            json.dump(credentials, f, indent=2)
        
        print(f"Created {self.credentials_path} file successfully!")
    
    def get_service(self, api_name, api_version, force_refresh=False):
        """Get a service client for the specified API."""
        service_key = f"{api_name}_{api_version}"
        
        # Return cached service if available
        if not force_refresh and service_key in self.services:
            return self.services[service_key]
        
        # Define required scopes based on the API
        scopes = self._get_scopes_for_api(api_name)
        
        # Authenticate if not already done
        if not self.creds:
            self.creds = self.authenticate(scopes)
            if not self.creds:
                return None
        
        # Build and cache the service
        try:
            service = build(api_name, api_version, credentials=self.creds)
            self.services[service_key] = service
            return service
        except HttpError as error:
            print(f"Error building {api_name} service: {error}")
            return None
    
    def _get_scopes_for_api(self, api_name):
        """Return the appropriate scopes for the requested API."""
        scope_map = {
            'calendar': ['https://www.googleapis.com/auth/calendar'],
            'drive': ['https://www.googleapis.com/auth/drive'],
            'gmail': ['https://www.googleapis.com/auth/gmail.modify'],
            'sheets': ['https://www.googleapis.com/auth/spreadsheets'],
            'tasks': ['https://www.googleapis.com/auth/tasks'],
            'docs': ['https://www.googleapis.com/auth/documents'],
            'people': ['https://www.googleapis.com/auth/contacts'],
            'youtube': ['https://www.googleapis.com/auth/youtube'],
            'analytics': ['https://www.googleapis.com/auth/analytics'],
            'classroom': ['https://www.googleapis.com/auth/classroom.courses'],
            'fitness': ['https://www.googleapis.com/auth/fitness.activity.read'],
            # Add more APIs as needed
        }
        
        # Default to calendar if API not in map
        return scope_map.get(api_name, ['https://www.googleapis.com/auth/calendar'])


class CalendarManager:
    """Class to manage Google Calendar operations."""
    
    def __init__(self, connector):
        """Initialize with a GoogleServicesConnector."""
        self.connector = connector
        self.service = connector.get_service('calendar', 'v3')
    
    def create_event(self, summary, description, start_date, end_date=None, 
                    all_day=True, location=None, reminders=None):
        """Create a calendar event."""
        if not self.service:
            print("Calendar service not available")
            return None
            
        event = {
            'summary': summary,
            'description': description,
            'location': location,
        }

        # Set date/time formatting
        if all_day:
            # For all-day events
            if end_date is None:
                # If no end date is provided, make it the same as start date for a one-day event
                end_date = start_date
            
            # For all-day events, we need to add one day to the end date
            # because Google Calendar's end date is exclusive
            end_date_obj = datetime.datetime.fromisoformat(end_date)
            end_date_obj = end_date_obj + datetime.timedelta(days=1)
            end_date = end_date_obj.strftime('%Y-%m-%d')
            
            event['start'] = {'date': start_date}
            event['end'] = {'date': end_date}
        else:
            # For time-specific events
            event['start'] = {'dateTime': start_date, 'timeZone': 'America/New_York'}
            event['end'] = {'dateTime': end_date, 'timeZone': 'America/New_York'}

        # Set reminders
        if reminders is None:
            reminders = {
                'useDefault': False,
                'overrides': [
                    {'method': 'email', 'minutes': 24 * 60},  # 1 day before
                    {'method': 'popup', 'minutes': 24 * 60},  # 1 day before
                    {'method': 'popup', 'minutes': 7 * 24 * 60}  # 1 week before
                ],
            }
        event['reminders'] = reminders

        try:
            event = self.service.events().insert(calendarId='primary', body=event).execute()
            print(f"Event created: {summary} ({event.get('htmlLink')})")
            return event
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None
    
    def list_upcoming_events(self, max_results=10):
        """List upcoming calendar events."""
        if not self.service:
            print("Calendar service not available")
            return []
            
        try:
            now = datetime.datetime.utcnow().isoformat() + 'Z'  # 'Z' indicates UTC time
            events_result = self.service.events().list(
                calendarId='primary',
                timeMin=now,
                maxResults=max_results,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            return events_result.get('items', [])
        except HttpError as error:
            print(f"An error occurred: {error}")
            return []


def add_marathon_events():
    """Add marathon registration deadlines to Google Calendar."""
    # Initialize our connector
    connector = GoogleServicesConnector()
    
    # Initialize calendar manager
    calendar = CalendarManager(connector)
    
    # Define marathon events focused on lottery/registration open dates
    marathon_events = [
        {
            'summary': 'Boston Marathon 2026 - Registration Month',
            'description': 'Registration for the 2026 Boston Marathon typically opens in September. Monitor the BAA website for exact dates. Race date: April 20, 2026.',
            'start_date': '2025-09-01',
            'location': 'Boston, MA'
        },
        {
            'summary': 'London Marathon 2026 - Lottery Opens',
            'description': 'The ballot for the 2026 London Marathon is expected to open late April 2025. Race date: April 26, 2026.',
            'start_date': '2025-04-01',
            'location': 'London, UK'
        },
        {
            'summary': 'Tokyo Marathon 2026 - Lottery Month',
            'description': 'The general lottery for the 2026 Tokyo Marathon is expected to open in August 2025. Race date: Early March 2026.',
            'start_date': '2025-08-01',
            'location': 'Tokyo, Japan'
        },
        {
            'summary': 'Berlin Marathon 2026 - Lottery Month',
            'description': 'The lottery for the 2026 Berlin Marathon typically opens in October. Race date: Late September 2026.',
            'start_date': '2025-10-01',
            'location': 'Berlin, Germany'
        },
        {
            'summary': 'Chicago Marathon 2026 - Lottery Month',
            'description': 'The lottery for the 2026 Chicago Marathon typically opens in October-November. Race date: October 11, 2026.',
            'start_date': '2025-10-01',
            'location': 'Chicago, IL'
        },
        {
            'summary': 'NYC Marathon 2026 - Lottery Month',
            'description': 'The lottery for the 2026 NYC Marathon typically opens in February. Race date: Early November 2026.',
            'start_date': '2026-02-01',
            'location': 'New York, NY'
        },
        {
            'summary': 'Sydney Marathon 2026 - Registration Month',
            'description': 'Registration for the 2026 Sydney Marathon is expected to open in early 2026. Race date: Late August/Early September 2026.',
            'start_date': '2026-01-01',
            'location': 'Sydney, Australia'
        }
    ]
    
    # Add each event to the calendar
    for event_data in marathon_events:
        calendar.create_event(
            event_data['summary'],
            event_data['description'],
            event_data['start_date'],
            end_date=event_data.get('end_date'),
            all_day=True,
            location=event_data.get('location')
        )
    
    print("All marathon registration deadlines have been added to your Google Calendar!")
def add_outdoor_movie_events():
    """Add outdoor movie events to your Google Calendar."""
    print("Adding outdoor movie events to your Google Calendar...")
    
    # Get the calendar service
    service = get_calendar_service()
    if not service:
        return
    
    # Define outdoor movie events for 2025
    outdoor_movie_events = [
        # Central Park Film Festival 2025 events
        {
            'summary': 'Central Park Film Festival: Opening Night',
            'description': 'The annual Central Park Film Festival begins tonight at sunset. Bring a blanket and picnic to enjoy outdoor movies under the stars.',
            'start_datetime': '2025-07-15T19:30:00',
            'location': 'Rumsey Playfield, Central Park, New York, NY'
        },
        {
            'summary': 'Central Park Film Festival: Family Night',
            'description': 'Family-friendly movie screening in Central Park. Film title TBA. Visit centralpark.com for updates.',
            'start_datetime': '2025-07-16T19:30:00',
            'location': 'Rumsey Playfield, Central Park, New York, NY'
        },
        {
            'summary': 'Central Park Film Festival: Classic Night',
            'description': 'Classic film screening in Central Park. Film title TBA. Visit centralpark.com for updates.',
            'start_datetime': '2025-07-17T19:30:00',
            'location': 'Rumsey Playfield, Central Park, New York, NY'
        },
        {
            'summary': 'Central Park Film Festival: Closing Night',
            'description': 'Final night of the Central Park Film Festival. Film title TBA. Visit centralpark.com for updates.',
            'start_datetime': '2025-07-18T19:30:00',
            'location': 'Rumsey Playfield, Central Park, New York, NY'
        },
        
        # Carl Schurz Park Movies 2025
        {
            'summary': 'Movies Under the Stars: The Wild Robot',
            'description': 'Outdoor movie screening of "The Wild Robot" at Carl Schurz Park. Bring a blanket or chair. Movie begins at sunset.',
            'start_datetime': '2025-06-18T20:00:00',
            'location': 'Carl Schurz Pickleball Court, Carl Schurz Park, Manhattan, NY'
        },
        {
            'summary': 'Movies Under the Stars at Carl Schurz Park',
            'description': 'Outdoor movie screening at Carl Schurz Park. Film TBA. Movies are held in the Basketball/Hockey/Pickleball Court at sunset. Limited seating available.',
            'start_datetime': '2025-07-09T20:00:00',
            'location': 'Carl Schurz Park, Manhattan, NY'
        },
        {
            'summary': 'Movies Under the Stars at Carl Schurz Park',
            'description': 'Outdoor movie screening at Carl Schurz Park. Film TBA. Movies are held in the Basketball/Hockey/Pickleball Court at sunset. Limited seating available.',
            'start_datetime': '2025-07-23T20:00:00',
            'location': 'Carl Schurz Park, Manhattan, NY'
        },
        {
            'summary': 'Movies Under the Stars at Carl Schurz Park',
            'description': 'Outdoor movie screening at Carl Schurz Park. Film TBA. Movies are held in the Basketball/Hockey/Pickleball Court at sunset. Limited seating available.',
            'start_datetime': '2025-08-06T20:00:00',
            'location': 'Carl Schurz Park, Manhattan, NY'
        },
        {
            'summary': 'Movies Under the Stars at Carl Schurz Park - Season Finale',
            'description': 'Final outdoor movie screening of the season at Carl Schurz Park. Film TBA. Movies are held in the Basketball/Hockey/Pickleball Court at sunset. Limited seating available.',
            'start_datetime': '2025-08-20T20:00:00',
            'location': 'Carl Schurz Park, Manhattan, NY'
        }
    ]
    
    # Add each event to the calendar
    for event_data in outdoor_movie_events:
        create_timed_event(
            service,
            event_data['summary'],
            event_data['description'],
            event_data['start_datetime'],
            end_datetime=event_data.get('end_datetime'),
            location=event_data.get('location')
        )
    
    print("All outdoor movie events have been added to your Google Calendar!")
    print("Note: Exact movie titles and specific dates may be updated closer to the events.")
    print("Each event has a reminder set for 1 day before the event.")

def main():
    """Main function to choose which events to add."""
    print("Google Calendar Event Creator")
    print("============================")
    print("1. Add Marathon Registration Deadlines")
    print("2. Add Outdoor Movie Events")
    print("3. Add Both")
    
    choice = input("Enter your choice (1-3): ")
    
    if choice == '1':
        add_marathon_events()
    elif choice == '2':
        add_outdoor_movie_events()
    elif choice == '3':
        add_marathon_events()
        add_outdoor_movie_events()
    else:
        print("Invalid choice. Exiting.")
# Example of using the drive service (you can uncomment when needed)
# def list_drive_files():
#     connector = GoogleServicesConnector()
#     drive_service = connector.get_service('drive', 'v3')
#     
#     try:
#         results = drive_service.files().list(
#             pageSize=10, fields="nextPageToken, files(id, name)").execute()
#         items = results.get('files', [])
#         
#         if not items:
#             print('No files found.')
#             return
#         
#         print('Files:')
#         for item in items:
#             print(f"{item['name']} ({item['id']})")
#     except HttpError as error:
#         print(f'An error occurred: {error}')

if __name__ == '__main__':
    # You can run different functions based on command line arguments
    # or just uncomment the function you want to use
    add_marathon_events()
    # list_drive_files()  # Uncomment to use Drive API