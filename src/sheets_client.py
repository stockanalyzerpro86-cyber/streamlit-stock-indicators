from __future__ import annotations

from typing import List, Dict, Any, Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def build_sheets_service(service_account_info: dict):
    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def get_values(service, spreadsheet_id: str, a1_range: str) -> List[List[Any]]:
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=a1_range
    ).execute()
    return resp.get("values", [])


def write_values(service, spreadsheet_id: str, a1_range: str, values: List[List[Any]]):
    body = {"values": values}
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=a1_range,
        valueInputOption="RAW",
        body=body
    ).execute()


def batch_update(service, spreadsheet_id: str, requests: List[Dict[str, Any]]):
    body = {"requests": requests}
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()
