import requests
from datetime import datetime
import time
import pandas as pd
from config.settings import API_KEY

BASE_URL = "https://prologapp.com/prolog/api/v3/"
HEADERS = {"x-prolog-api-token": API_KEY}


def extract_users(branch_office_id=1074):
    url = BASE_URL + "users"
    user_ids = []
    params = {"branchOfficesId": branch_office_id, "pageSize": 100, "pageNumber": 0}
    while True:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200 or not response.json().get("content"):
            break
        user_ids += [u["id"] for u in response.json()["content"]]
        params["pageNumber"] += 1
        time.sleep(0.5)
    details = []
    for uid in user_ids:
        r = requests.get(f"{url}/{uid}", headers=HEADERS)
        if r.status_code == 200:
            details.append(r.json())
        time.sleep(0.3)
    return details


def extract_checklists(branch_office_id=1074, start_date="2025-01-01T00:00Z"):
    url = BASE_URL + "checklists"
    end_date = datetime.utcnow().strftime("%Y-%m-%dT23:59Z")
    all_data = []
    params = {
        "branchOfficesId": [branch_office_id],
        "includeInactive": "true",
        "pageSize": 100,
        "startDate": start_date,
        "endDate": end_date,
        "includeAnswers": "true",
        "pageNumber": 0
    }
    for _ in range(100):
        r = requests.get(url, headers=HEADERS, params=params)
        if r.status_code != 200:
            break
        page_data = r.json().get("content", [])
        if not page_data:
            break
        all_data.extend(page_data)
        params["pageNumber"] += 1
        time.sleep(0.8)
    return all_data


def extract_vehicles(branch_office_id=1074):
    url = BASE_URL + "vehicles"
    params = {
        "branchOfficesId": branch_office_id,
        "includeInactive": "true",
        "pageSize": "100",
        "pageNumber": "0",
        "startDate": "2024-06-01T00:00Z",
        "endDate": "2025-09-18T23:59Z"
    }
    r = requests.get(url, headers=HEADERS, params=params)
    return r.json().get("content", []) if r.status_code == 200 else []


def extract_os(branch_office_id=1074):
    base_url = BASE_URL + "work-orders"
    ids = []
    params = {"branchOfficesId": branch_office_id, "pageSize": 100, "pageNumber": 0}
    while True:
        r = requests.get(base_url, headers=HEADERS, params=params)
        if r.status_code != 200 or not r.json().get("content"):
            break
        ids.extend([o["internalWorkOrderId"] for o in r.json()["content"]])
        params["pageNumber"] += 1
        time.sleep(0.5)
    details = []
    for oid in ids:
        r = requests.get(f"{base_url}/{oid}", headers=HEADERS)
        if r.status_code == 200:
            order = r.json()
            order["itemServices"] = len(order.get("itemServices", []))
            order["itemProducts"] = len(order.get("itemProducts", []))
            details.append(order)
        time.sleep(0.5)
    return details