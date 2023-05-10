from __future__ import print_function
from dotenv import load_dotenv
import os
from pyodk.client import Client
import pandas as pd
from geopy import Nominatim, distance
from routingpy import ORS
from time import sleep
from datetime import datetime
from geopy.exc import GeocoderUnavailable, GeocoderTimedOut
from routingpy.exceptions import RouterApiError
import logging
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


log = logging.getLogger(__name__)


def append_values(spreadsheet_id, range_name, value_input_option, values):
    """
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    creds, _ = google.auth.default()
    # pylint: disable=maybe-no-member
    try:
        service = build("sheets", "v4", credentials=creds)

        body = {"values": values}
        result = (
            service.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body,
            )
            .execute()
        )
        print(f"{(result.get('updates').get('updatedCells'))} cells appended.")
        return result

    except HttpError as error:
        print(f"An error occurred: {error}")
        return error
    


def get_values(spreadsheet_id, range_name):
    """
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
        """
    creds, _ = google.auth.default()
    # pylint: disable=maybe-no-member
    try:
        service = build('sheets', 'v4', credentials=creds)

        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name).execute()
        rows = result.get('values', [])
        print(f"{len(rows)} rows retrieved")
        return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error
    

def get_data_from_central(last_date):
    client = Client(config_path=os.environ.get("PYODK_CONFIG_FILE"))
    form_data = client.submissions.get_table(
        form_id="trajets-voyage",
        project_id=1,
        filter=f"__system/submissionDate gt {last_date}",
    )
    df = pd.DataFrame(form_data["value"])
    df["orig"] = df["orig_country"] + ", " + df["origin_adress"]
    df["dest"] = df["dest_country"] + ", " + df["dest_adress"]
    return df


def get_distance(row):
    sleep(1)
    log.info(f"{row['origin_adress']} {row['dest_adress']} ")
    geolocator = Nominatim(user_agent="bilancarbonevoyage")
    try:

        orig = geolocator.geocode(row["orig"], timeout=1000)
        dest = geolocator.geocode(row["dest"], timeout=1000)
        if orig and dest:
            coords = [[orig.longitude, orig.latitude], [dest.longitude, dest.latitude]]

            if row["mode"] not in ("Lancha", "SpeedBoat"):
                ors = ORS(os.environ.get("ORS_KEY"))
                profiles = {
                    "Bus": "driving-car",
                    "Minibus": "driving-car",
                    "SpeedBoat": "foot-walking",
                }
                try:
                    d = (
                        ors.directions(
                            locations=coords,
                            profile=profiles.get(row["mode"], "driving-car"),
                        ).distance
                        / 1000
                    )
                except RouterApiError:
                    d = 0
            else:
                d = (
                    distance.distance(
                        (orig.latitude, orig.longitude), (dest.latitude, dest.longitude)
                    ).m
                    / 1000
                )
        else:
            d = 0
    except GeocoderUnavailable or GeocoderTimedOut or RouterApiError:
        d = 0
    return d


def main():
    load_dotenv()
    SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
    dates = get_values(SPREADSHEET_ID, "I:I")
    LAST_DATE = str(max([pd.to_datetime(d[0], format='%Y-%m-%d') for d in dates['values'] if d[0] != "Date"]).date())

    df = get_data_from_central(LAST_DATE) 
    df = df[pd.to_datetime(df['date']) > LAST_DATE] # need this because data from central is filtered on submission date not trip date
    if not df.empty:
        df["distances"] = df.apply(get_distance, axis=1)
        df["complex"] = [1 if dist == 0 else 0 for dist in df["distances"]]
        df["Trip"] = ""
        to_append = df[
            [
                "Trip",
                "complex",
                "orig_country",
                "origin_adress",
                "dest_country",
                "dest_adress",
                "distances",
                "mode",
                "date",
            ]
        ].values.tolist()
        append_values(SPREADSHEET_ID, "C:I", "USER_ENTERED", to_append)
        log.info(f'Appended {len(df)} rows')
    else:
        log.info('Not new data ')
        


if __name__ == "__main__":
    main()
