import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SAMPLE_SPREADSHEET_ID = "1BeOZFEa00aC8jGFB_jH_KCT-Y_YoLDqlHtQqJtoed7I"
OUTPUT_DIR = "output_data"


def get_all_sheets(service, spreadsheet_id):
    """Fetches all sheet names in a spreadsheet."""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get("sheets", [])
        return [sheet["properties"]["title"] for sheet in sheets]
    except HttpError as err:
        print(f"An error occurred while fetching sheet names: {err}")
        return []


def read_google_sheet(service, sheet_name):
    """Fetches data from a specific sheet in Google Sheets."""
    try:
        range_name = f"{sheet_name}!A1:Z"
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=range_name)
            .execute()
        )
        return result.get("values", [])  # Return raw rows
    except HttpError as err:
        print(f"An error occurred while accessing sheet {sheet_name}: {err}")
        return []


def read_previous_data(sheet_name):
    """Reads previously saved data for a sheet."""
    file_path = os.path.join(OUTPUT_DIR, f"{sheet_name}_latest.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    return []


def write_new_data(sheet_name, data):
    """Writes updated data for a sheet."""
    file_path = os.path.join(OUTPUT_DIR, f"{sheet_name}_latest.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def save_changes(sheet_name, changes):
    """Saves only changes for a sheet."""
    changes_count = len([f for f in os.listdir(OUTPUT_DIR) if f.startswith(f"changes_{sheet_name}_")])
    changes_file = os.path.join(OUTPUT_DIR, f"changes_{sheet_name}_{changes_count + 1}.json")
    with open(changes_file, "w") as f:
        json.dump(changes, f, indent=4, ensure_ascii=False)
    print(f"Changes saved to {changes_file}")


def process_sheet(service, sheet_name):
    """Processes a single sheet, saving updated and changed data."""
    print(f"Processing sheet: {sheet_name}")
    current_data = read_google_sheet(service, sheet_name)
    if not current_data:
        print(f"No data found in sheet: {sheet_name}")
        return

    previous_data = read_previous_data(sheet_name)
    new_data = [row for row in current_data if row not in previous_data]

    if new_data:
        print(f"New/changed rows found in {sheet_name}: {len(new_data)}")

        # Save the full updated data
        write_new_data(sheet_name, current_data)

        # Save only the changes
        save_changes(sheet_name, new_data)
    else:
        print(f"No new changes found in {sheet_name}.")


def main():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    service = build("sheets", "v4", credentials=creds)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sheet_names = get_all_sheets(service, SAMPLE_SPREADSHEET_ID)
    if not sheet_names:
        print("No sheets found in the spreadsheet.")
        return

    for sheet_name in sheet_names:
        process_sheet(service, sheet_name)


if __name__ == "__main__":
    main()
