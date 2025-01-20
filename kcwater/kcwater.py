import aiohttp
import asyncio
from datetime import date, timedelta, datetime
import logging
from os import getenv

from typing import List, Optional
from dataclasses import dataclass


@dataclass
class Reading:
    readDateTime: datetime
    uom: str
    meterNumber: Optional[str]
    gallonsConsumption: str
    rawConsumption: str
    scaledRead: str
    port: str


log_level = logging.DEBUG
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s -  - %(message)s", level=log_level
)

day_before_yesterday = date.today() - timedelta(days=2)
yesterday = date.today() - timedelta(days=1)
today = date.today()
now = datetime.now()


def valid_charge_date(history_obj):
    """Compares a date to today to see if it's in the past."""
    date_string = history_obj["chargeDateRaw"]
    d = None
    try:
        d = datetime.strptime(date_string, "%d-%b-%Y")
    except ValueError:
        d = datetime.strptime(date_string, "%m-%d-%Y")
    valid_date = d.date() < today

    if d.date() == today and history_obj["readDateTime"]:
        time_split = history_obj["readDateTime"].split(" ")
        d_hour = (
            int(time_split[0]) if time_split[1] == "AM" else (int(time_split[0]) + 12)
        )
        d = d + timedelta(hours=d_hour + 1)  # don't want to include current hour
        return d <= now

    return valid_date


def strip_future_data(dataset):
    return list(filter(valid_charge_date, dataset))


class KCWater:
    def __init__(self, session: aiohttp.ClientSession, username, password):
        self.loggedIn = False
        self.session: aiohttp.ClientSession = session
        self.headers = {}
        self.username: str = username
        self.password: str = password
        self.account_number = None
        self.customer_id = None
        self.service_id = None
        self.access_token = None
        self.account_port = 1
        self.tokenUrl = "https://my.kcwater.us/rest/oauth/token"
        self.customer_info_url = "https://my.kcwater.us/rest/account/customer/"
        self.hourly_usage_url = "https://my.kcwater.us/rest/usage/month/day"
        self.daily_usage_url = "https://my.kcwater.us/rest/usage/month"

    async def _get_token(self):
        logging.info("Logging in with username: " + self.username)
        login_payload = {
            "username": str(self.username),
            "password": str(self.password),
            "grant_type": "password",
        }
        headers = {"Authorization": "Basic d2ViQ2xpZW50SWRQYXNzd29yZDpzZWNyZXQ="}
        async with self.session.post(
            self.tokenUrl, headers=headers, data=login_payload
        ) as response:
            login_data = await response.json()
            logging.debug("Login response: " + str(response.status))
            self.access_token = login_data["access_token"]
            self.customer_id = login_data["user"]["customerId"]
            self.headers["Authorization"] = "Bearer {}".format(self.access_token)
            self.headers["Content-Type"] = "application/json"

    async def _get_customer_info(self):
        info_payload = {"customerId": str(self.customer_id)}
        async with self.session.post(
            self.customer_info_url, json=info_payload, headers=self.headers
        ) as response:
            logging.debug("Customer Info response: " + str(response.status))
            customer_info = await response.json()
            self.service_id = customer_info["accountSummaryType"]["services"][0][
                "serviceId"
            ]
            self.account_number = customer_info["accountContext"]["accountNumber"]

    async def login(self):
        await self._get_token()
        await self._get_customer_info()
        self.loggedIn = (
            self.account_number is not None
            and self.service_id is not None
            and self.customer_id is not None
            and self.access_token is not None
        )

    async def get_usage_hourly(self, date=today) -> Optional[List[Reading]]:
        """Fetches all usage data for a given date by hour."""
        if not self.loggedIn:
            logging.error("Must login first")
            return None
        formatted_date = date.strftime("%d-%b-%Y")
        req_payload = {
            "customerId": str(self.customer_id),
            "accountContext": {
                "accountNumber": str(self.account_number),
                "serviceId": str(self.service_id),
            },
            "month": formatted_date,
            "day": formatted_date,
            "port": str(self.account_port),
        }

        async with self.session.post(
            self.hourly_usage_url, json=req_payload, headers=self.headers
        ) as response:
            usageData = await response.json()
            readings: List[Reading] = []
            for r in usageData["history"]:
                readDate = datetime.strptime(
                    f"{r['readDate']} {r['readDateTime']}", "%m-%d-%Y %I %p"
                )

                reading = Reading(
                    readDateTime=readDate,
                    uom=r["uom"],
                    meterNumber=r["meterNumber"],
                    rawConsumption=r["rawConsumption"],
                    port=r["port"],
                    gallonsConsumption=r["gallonsConsumption"],
                    scaledRead=r["scaledRead"],
                )
                readings.append(reading)

            return readings

    # async def get_usage_daily(self, date=yesterday):
    #     """Fetches all usage data from the given month by day."""
    #     if not self.loggedIn:
    #         logging.error("Must login first")
    #         return
    #
    #     formatted_date = date.strftime("%d-%b-%Y")
    #     req_payload = {
    #         "customerId": str(self.customer_id),
    #         "accountContext": {
    #             "accountNumber": str(self.account_number),
    #             "serviceId": str(self.service_id),
    #         },
    #         "month": formatted_date,
    #     }
    #
    #     async with self.session.post(
    #         self.daily_usage_url, json=req_payload, headers=self.headers
    #     ) as response:
    #         usageData = await response.json()
    #         return DailyReadings(daily_readings=usageData["history"])
    #         # return strip_future_data(usageData["history"])


def getCreds():
    return {
        "username": getenv("KCWATER_USERNAME"),
        "password": getenv("KCWATER_PASSWORD"),
    }


if __name__ == "__main__":
    # Read the credentials.json file
    async def sample():
        creds = getCreds()
        username = creds["username"]
        password = creds["password"]

        session = aiohttp.ClientSession()
        kc_water = KCWater(session, username, password)
        await kc_water.login()
        logging.debug(
            "Account Number = {}, service ID = {}, customer ID = {}".format(
                kc_water.account_number, kc_water.service_id, kc_water.customer_id
            )
        )

        # Get a list of hourly readings
        hourly_data = await kc_water.get_usage_hourly()
        logging.info("Hourly data: {}\n\n".format(hourly_data))

        # Get a list of hourly readings
        # daily_data = await kc_water.get_usage_daily()
        # logging.info("Daily data: {}\n\n".format(daily_data))

        # logging.info("Last daily data: {}\n\n".format(daily_data[-1]))
        # logging.info("Last hourly data: {}\n\n".format(hourly_data[-1]))
        # logging.info(
        #     "Last daily reading: {} gal for {}".format(
        #         daily_data[-1]["gallonsConsumption"], daily_data[-1]["readDate"]
        #     )
        # )
        # logging.info(
        #     "Last hourly reading: {} gal for {} {}".format(
        #         hourly_data[-1]["gallonsConsumption"],
        #         hourly_data[-1]["readDate"],
        #         hourly_data[-1]["readDateTime"],
        #     )
        # )
        await session.close()

    asyncio.run(sample())
