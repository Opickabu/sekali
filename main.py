import argparse
import asyncio
import json
import random
from contextlib import suppress
from datetime import datetime
from http import HTTPStatus
from itertools import cycle
import sys
import requests

from time import time
from urllib.parse import unquote

import aiohttp
import pytz
from aiocfscrape import CloudflareScraper
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy

from bot.config import settings
from bot.core.agents import generate_random_user_agent
from bot.core.registrator import register_query_id
from bot.core.TLS import TLSv1_3_BYPASS
from bot.exceptions import (
    ErrorStartGameException,
    ExpiredTokenException,
    GameSessionNotFoundException,
    InvalidProtocol,
    InvalidSessionException,
)
from bot.utils import logger
from bot.utils.boosts import FreeBoostType, UpgradableBoostType
from bot.utils.graphql import OperationName, Query
from helpers import (
    calculate_spin_multiplier,
    check_complete_task_delay,
    convert_datetime_str_to_utc,
    format_duration,
    get_query_ids,
    get_tele_user_obj_from_query_id,
    bcolors,
)

curr_version = "2.0.2"

banner = f"""
========================================================================================
                                    {bcolors.BOLD}MEMEFI BOT{bcolors.ENDC}
========================================================================================
Version: {bcolors.OKCYAN}{curr_version}{bcolors.ENDC}
Created by : https://t.me/irhamdz (Irham Dzuhri)
You can only get this premium bot from Dzuhri Auto Repository or directly from the creator.
Any other source is fake!
========================================================================================
"""


# def check_license_key():
#     if not settings.LICENSE_KEY:
#         raise MissingApiKeyException("LICENSE KEY is missing, please check your .env file!")


def check_version():
    version = requests.get(
        "https://raw.githubusercontent.com/dzuhri-auto/memefi/refs/heads/main/version"
    )
    version_ = version.text.strip()
    if curr_version != version_:
        logger.warning(
            f"<ly>New version detected: <lc>{version_}</lc>, Please update the bot by running the following command:</ly> <lg>git pull</lg>"
        )
        sys.exit()


def create_menus():
    menus = ["Start bot", "Add Session", "Delete Session"]
    print("Please choose menu: ")
    print("")
    total_menu = 0
    for idx, menu in enumerate(menus):
        num = idx + 1
        total_menu += 1
        print(f"{num}. {menu}")
    print(
        "========================================================================================"
    )
    return total_menu


def get_proxies() -> list[Proxy]:
    if settings.USE_PROXY_FROM_FILE.lower() == "true":
        with open(file="bot/config/proxies.txt", encoding="utf-8-sig") as file:
            proxies = [Proxy.from_str(proxy=row.strip()).as_url for row in file]
    else:
        proxies = []
    return proxies


async def delete_account():
    delete = True
    while delete:
        query_ids = await get_query_ids()
        number_validation = []
        list_of_username = []
        delete_action = None

        if query_ids:
            print("")
            print("Please select the session you want to delete. (press enter to exit): ")
            print("")
            for idx, query_id in enumerate(query_ids):
                tele_user_obj = get_tele_user_obj_from_query_id(query_id)
                username = tele_user_obj.get("username")
                num = idx + 1
                print(f"{num}. {username}")
                list_of_username.append(username)
                number_validation.append(str(num))
            print("")

            while True:
                delete_action = input("> ")
                if not delete_action:
                    return None

                if not delete_action.isdigit():
                    logger.warning("Please only input number")
                elif delete_action not in number_validation:
                    logger.warning("Please only input number that are available")
                else:
                    delete_action = int(delete_action)
                    break

            with open("query_ids.txt", "r+") as f:
                content = f.readlines()
                content_len = len(content)
                f.truncate(0)
                f.seek(0)
                index_to_strip = 0
                for content_idx, line in enumerate(content):
                    if not content_idx == (delete_action - 1):
                        if delete_action == content_len:
                            index_to_strip = delete_action - 2
                        if index_to_strip and content_idx == index_to_strip:
                            f.write(line.strip())
                        else:
                            f.write(line)

            logger.success(f"Successfully delete session: {list_of_username[delete_action - 1]}")

            list_of_username.pop(delete_action - 1)

            if not list_of_username:
                logger.success(f"No session left")
                return None

            print("\n")
            keep_deleting = input("Do you want to delete another? (y/n) > ")
            if not keep_deleting or keep_deleting == "n":
                return None
            elif keep_deleting == "y":
                continue
            else:
                return None
        else:
            logger.warning(
                "No query ID found. Please select <lc>Add Session</lc> or add it directly to the <lc>query_ids.txt</lc> file"
            )
            return None


async def process() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--action", type=int, help="Action to perform")
    action = parser.parse_args().action
    if not action:
        print(banner)
        total_menu = create_menus()
        list_of_menu_str = [str(num + 1) for num in range(total_menu)]
        while True:
            action = input("> ")

            if not action.isdigit():
                logger.warning("Please only input number")
            elif action not in list_of_menu_str:
                logger.warning("Please only input number that are available")
            else:
                action = int(action)
                break

    if action == 2:
        await register_query_id()
    if action == 1:
        await run_tasks()
    elif action == 3:
        await delete_account()


async def run_tasks():
    query_ids = await get_query_ids()
    if not query_ids:
        logger.warning(
            "No query ID found. Please select <lc>Add Session</lc> or add it directly to the <lc>query_ids.txt</lc> file"
        )
        return
    proxies = get_proxies()
    logger.info(f"============================================================")
    logger.info(f"Detected <lc>{len(query_ids)}</lc> accounts | <lc>{len(proxies)}</lc> proxies")
    logger.info(f"============================================================")
    proxies_cycle = cycle(proxies) if proxies else None
    tasks = [
        asyncio.create_task(
            run_tapper(
                query_id=query_id,
                proxy=next(proxies_cycle) if proxies_cycle else None,
            )
        )
        for query_id in query_ids
    ]
    await asyncio.gather(*tasks)


class Tapper:
    def __init__(self, query_id: str):
        self.query_id = query_id
        self.user_id = 0
        self.username = None
        self.first_name = None
        self.last_name = None
        self.fullname = None
        self.start_param = None
        self.peer = None
        self.first_run = None
        self.login_base_url = "https://gateway.blum.codes/v1/auth/provider"
        self.base_url = "https://game-domain.blum.codes/api/v1"
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": "https://tg-app.memefi.club",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Sec-Ch-Ua": '"Google Chrome";v="127", "Chromium";v="127", "Not.A/Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?1",
            "Sec-Ch-Ua-Platform": "Android",
        }
        self.session_ug_dict = self.load_user_agents() or []
        self.access_token_created_time = 0
        self.GRAPHQL_URL = "https://api-gw-tg.memefi.club/graphql"

    def logger_error_from_exception(self, action, error):
        if error.status == HTTPStatus.BAD_REQUEST:
            self.warning(f"<ly>[{action}] - {error}</ly>")
        else:
            self.error(f"<ly>[{action}] - {error}</ly>")
        return None

    async def generate_random_user_agent(self):
        return generate_random_user_agent(device_type="android", browser_type="chrome")

    def info(self, message):
        from bot.utils import info

        info(f"<light-yellow>{self.session_name}</light-yellow> | {message}")

    def debug(self, message):
        from bot.utils import debug

        debug(f"<light-yellow>{self.session_name}</light-yellow> | ⚙ {message}")

    def warning(self, message):
        from bot.utils import warning

        warning(f"<light-yellow>{self.session_name}</light-yellow> | ⚠️ {message}")

    def error(self, message):
        from bot.utils import error

        error(f"<light-yellow>{self.session_name}</light-yellow> | ❌ {message}")

    def critical(self, message):
        from bot.utils import critical

        critical(f"<light-yellow>{self.session_name}</light-yellow> | ‼ {message}")

    def success(self, message):
        from bot.utils import success

        success(f"<light-yellow>{self.session_name}</light-yellow> | ✅ {message}")

    def save_user_agent(self):
        user_agents_file_name = "user_agents.json"
        if not any(
            session["session_name"] == self.session_name for session in self.session_ug_dict
        ):
            user_agent_str = generate_random_user_agent()
            self.session_ug_dict.append(
                {"session_name": self.session_name, "user_agent": user_agent_str}
            )
            with open(user_agents_file_name, "w") as user_agents:
                json.dump(self.session_ug_dict, user_agents, indent=4)
            self.success("User agent saved successfully")
            return user_agent_str

    def load_user_agents(self):
        user_agents_file_name = "user_agents.json"
        try:
            with open(user_agents_file_name, "r") as user_agents:
                session_data = json.load(user_agents)
                if isinstance(session_data, list):
                    return session_data
        except FileNotFoundError:
            logger.warning("User agents file not found, creating...")
        except json.JSONDecodeError:
            logger.warning("User agents file is empty or corrupted.")
        return []

    def check_user_agent(self):
        load = next(
            (
                session["user_agent"]
                for session in self.session_ug_dict
                if session["session_name"] == self.session_name
            ),
            None,
        )
        if load is None:
            return self.save_user_agent()
        return load

    async def get_access_token(self, http_client: aiohttp.ClientSession, tg_web_data: dict[str]):
        try:
            resp = await http_client.post(url=self.GRAPHQL_URL, json=tg_web_data)
            resp.raise_for_status()
            resp_json = await resp.json()
            access_token = resp_json["data"]["telegramUserLogin"]["access_token"]
            return access_token
        except Exception as error:
            self.error(f"get_access_token error {error}")

    async def get_profile_data(self, http_client: aiohttp.ClientSession):
        for _ in range(5):
            try:
                json_data = {
                    "operationName": OperationName.QUERY_GAME_CONFIG,
                    "query": Query.QUERY_GAME_CONFIG,
                    "variables": {},
                }

                response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
                response.raise_for_status()

                response_json = await response.json()

                if "errors" in response_json:
                    raise InvalidProtocol(
                        f'get_profile_data msg: {response_json["errors"][0]["message"]}'
                    )

                profile_data = response_json.get("data", {}).get("telegramGameGetConfig", {})

                if not profile_data:
                    await asyncio.sleep(delay=3)
                    continue

                return profile_data
            except Exception as error:
                self.error(f"Unknown error while getting Profile Data: {error}")
                await asyncio.sleep(delay=3)

        return {}

    async def get_bot_config(self, http_client: aiohttp.ClientSession):
        for _ in range(5):
            try:
                json_data = {
                    "operationName": OperationName.TapbotConfig,
                    "query": Query.TapbotConfig,
                    "variables": {},
                }

                response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
                response.raise_for_status()

                response_json = await response.json()

                if "errors" in response_json:
                    raise InvalidProtocol(
                        f'get_bot_config msg: {response_json["errors"][0]["message"]}'
                    )

                bot_config = response_json.get("data", {}).get("telegramGameTapbotGetConfig", {})

                if not bot_config:
                    await asyncio.sleep(delay=3)
                    continue

                return bot_config
            except Exception as error:
                self.error(f"Unknown error while getting TapBot Data: {error}")
                await asyncio.sleep(delay=3)

        return {}

    async def start_bot(self, http_client: aiohttp.ClientSession):
        for _ in range(5):
            try:
                json_data = {
                    "operationName": OperationName.TapbotStart,
                    "query": Query.TapbotStart,
                    "variables": {},
                }

                response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
                response.raise_for_status()

                response_json = await response.json()

                if "errors" in response_json:
                    raise InvalidProtocol(f'start_bot msg: {response_json["errors"][0]["message"]}')

                start_data = response_json["data"]["telegramGameTapbotStart"]

                if not start_data:
                    await asyncio.sleep(delay=3)
                    continue

                return start_data
            except Exception as error:
                self.error(f"Unknown error while Starting Bot: {error}")
                await asyncio.sleep(delay=3)

        return None

    async def claim_bot(self, http_client: aiohttp.ClientSession):
        for _ in range(5):
            try:
                json_data = {
                    "operationName": OperationName.TapbotClaim,
                    "query": Query.TapbotClaim,
                    "variables": {},
                }

                response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
                response.raise_for_status()

                response_json = await response.json()

                if "errors" in response_json:
                    raise InvalidProtocol(f'claim_bot msg: {response_json["errors"][0]["message"]}')

                claim_data = response_json.get("data", {}).get("telegramGameTapbotClaimCoins", {})

                if not claim_data:
                    await asyncio.sleep(delay=3)
                    continue

                return claim_data
            except Exception as error:
                self.error(f"Unknown error while Claiming Bot: {error}")
                await asyncio.sleep(delay=3)

        return {}

    async def set_next_boss(self, http_client: aiohttp.ClientSession):
        try:
            json_data = {
                "operationName": OperationName.telegramGameSetNextBoss,
                "query": Query.telegramGameSetNextBoss,
                "variables": {},
            }

            response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
            response.raise_for_status()

            response_json = await response.json()

            return True
        except Exception as error:
            self.error(f"Unknown error while Setting Next Boss: {error}")
            await asyncio.sleep(delay=3)

            return False

    async def apply_boost(self, http_client: aiohttp.ClientSession, boost_type: FreeBoostType):
        try:
            json_data = {
                "operationName": OperationName.telegramGameActivateBooster,
                "query": Query.telegramGameActivateBooster,
                "variables": {"boosterType": boost_type},
            }

            response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
            response.raise_for_status()

            response_json = await response.json()

            if "errors" in response_json:
                raise InvalidProtocol(f'apply_boost msg: {response_json["errors"][0]["message"]}')

            return True
        except Exception as error:
            self.error(f"Unknown error while Apply {boost_type} Boost: {error}")
            await asyncio.sleep(delay=3)

            return False

    async def play_slotmachine(self, http_client: aiohttp.ClientSession, spin_multiplier: int):
        try:
            json_data = {
                "operationName": OperationName.SlotMachineSpin,
                "query": Query.SpinSlotMachine,
                "variables": {"payload": {"spinsCount": spin_multiplier}},
            }

            response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
            response.raise_for_status()

            response_json = await response.json()

            play_data = response_json.get("data", {}).get("slotMachineSpinV2", {})

            return play_data
        except Exception as err:
            return {}

    async def upgrade_boost(
        self, http_client: aiohttp.ClientSession, boost_type: UpgradableBoostType
    ):
        try:
            json_data = {
                "operationName": OperationName.telegramGamePurchaseUpgrade,
                "query": Query.telegramGamePurchaseUpgrade,
                "variables": {"upgradeType": boost_type},
            }

            response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
            response.raise_for_status()

            response_json = await response.json()

            if "errors" in response_json:
                raise InvalidProtocol(f'upgrade_boost msg: {response_json["errors"][0]["message"]}')

            return True
        except Exception:
            return False

    async def send_taps(self, http_client: aiohttp.ClientSession, nonce: str, taps: int):
        for _ in range(5):
            try:
                vector = []

                for _ in range(taps):
                    vector.append(str(random.randint(1, 4)))

                vector = ",".join(vector)

                json_data = {
                    "operationName": OperationName.MutationGameProcessTapsBatch,
                    "query": Query.MutationGameProcessTapsBatch,
                    "variables": {
                        "payload": {
                            "nonce": nonce,
                            "tapsCount": taps,
                            "vector": vector,
                        },
                    },
                }

                response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
                response.raise_for_status()

                response_json = await response.json()

                if "errors" in response_json:
                    raise InvalidProtocol(f'send_taps msg: {response_json["errors"][0]["message"]}')

                profile_data = response_json.get("data", {}).get("telegramGameProcessTapsBatch", {})

                if not profile_data:
                    await asyncio.sleep(delay=3)
                    continue

                return profile_data
            except Exception as error:
                self.error(f"Unknown error when Tapping: {error}")
                await asyncio.sleep(delay=3)

        return {}

    async def start_tapbot(self, http_client: aiohttp.ClientSession, bot_config: dict):
        used_attempts = bot_config.get("usedAttempts", 0)
        total_attempts = bot_config.get("totalAttempts", 0)

        if used_attempts < total_attempts:
            logger.info(f"{self.session_name} | Sleep 5s before start the TapBot")
            await asyncio.sleep(5)

            start_data = await self.start_bot(http_client=http_client)
            if start_data:
                damage_per_sec = start_data.get("damagePerSec", 0)
                logger.success(
                    f"{self.session_name} | Successfully started TapBot | "
                    f"Damage per second: <le>{damage_per_sec}</le> points"
                )
        else:
            self.info(
                "TapBot attempts are spent | "
                f"<ly>{used_attempts}</ly><lw>/</lw><le>{total_attempts}</le>"
            )

    async def purchase_and_start_tapbot(self, http_client: aiohttp.ClientSession, bot_config: dict):
        status = await self.upgrade_boost(
            http_client=http_client, boost_type=UpgradableBoostType.TAPBOT
        )
        if status:
            self.success(f"Successfully purchased TapBot")
            await asyncio.sleep(1)
            await self.start_tapbot(http_client, bot_config)

    async def query_video_ad_task(self, http_client: aiohttp.ClientSession):
        try:
            json_data = [
                {
                    "operationName": OperationName.QueryVideoAdTask,
                    "variables": {},
                    "query": Query.QueryVideoAdTask,
                },
                {
                    "operationName": OperationName.getSocialTask,
                    "variables": {},
                    "query": Query.getSocialTask,
                },
                {
                    "operationName": OperationName.CampaignLists,
                    "variables": {},
                    "query": Query.CampaignLists,
                },
            ]

            response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
            response.raise_for_status()

            response_json = await response.json()

            if "errors" in response_json:
                raise InvalidProtocol(
                    f'query_video_ad_task msg: {response_json["errors"][0]["message"]}'
                )

            return True
        except Exception:
            return False

    async def get_campaign_list(self, http_client: aiohttp.ClientSession):
        try:
            json_data = [
                {
                    "operationName": OperationName.CampaignLists,
                    "variables": {},
                    "query": Query.CampaignLists,
                }
            ]

            response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
            response.raise_for_status()

            response_json = await response.json()

            if isinstance(response_json, dict) and "errors" in response_json:
                raise InvalidProtocol(f'verify_task msg: {response_json["errors"][0]["message"]}')
            if isinstance(response_json, list) and "errors" in response_json[0]:
                raise InvalidProtocol(
                    f'verify_task msg: {response_json[0]["errors"][0]["message"]}'
                )

            return response_json
        except Exception:
            return []

    async def get_campaign_task_list(self, http_client: aiohttp.ClientSession, campaign_id: str):
        try:
            response_json = []
            json_data = [
                {
                    "operationName": OperationName.GetTasksList,
                    "variables": {"campaignId": campaign_id},
                    "query": Query.GetTasksList,
                }
            ]

            response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
            response.raise_for_status()

            response_json = await response.json()

            if isinstance(response_json, dict) and "errors" in response_json:
                raise InvalidProtocol(f'verify_task msg: {response_json["errors"][0]["message"]}')
            if isinstance(response_json, list) and "errors" in response_json[0]:
                raise InvalidProtocol(
                    f'verify_task msg: {response_json[0]["errors"][0]["message"]}'
                )

            if response_json[0].get("data"):
                response_json = response_json[0].get("data", {}).get("campaignTasks", [])

            return response_json
        except Exception:
            return []

    async def get_task_by_id(self, http_client: aiohttp.ClientSession, task_id: str):
        try:
            response_json = None
            json_data = [
                {
                    "operationName": OperationName.GetTaskById,
                    "variables": {"taskId": task_id},
                    "query": Query.GetTaskById,
                },
                {
                    "operationName": OperationName.TwitterProfile,
                    "variables": {},
                    "query": Query.TwitterProfile,
                },
            ]

            response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
            response.raise_for_status()

            response_json = await response.json()

            if isinstance(response_json, dict) and "errors" in response_json:
                raise InvalidProtocol(f'verify_task msg: {response_json["errors"][0]["message"]}')
            if isinstance(response_json, list) and "errors" in response_json[0]:
                raise InvalidProtocol(
                    f'verify_task msg: {response_json[0]["errors"][0]["message"]}'
                )

            if response_json[0].get("data"):
                response_json = response_json[0].get("data", {}).get("campaignTaskGetConfig")

            return response_json
        except Exception:
            return None

    async def verify_task(self, http_client: aiohttp.ClientSession, task_config_id: str):
        try:
            json_data = [
                {
                    "operationName": OperationName.CampaignTaskToVerification,
                    "variables": {
                        "taskConfigId": task_config_id,
                    },
                    "query": Query.CampaignTaskToVerification,
                }
            ]

            async with http_client.post(url=self.GRAPHQL_URL, json=json_data) as response:
                response.raise_for_status()
                response_json = await response.json()

                if isinstance(response_json, dict) and "errors" in response_json:
                    raise InvalidProtocol(f'verify_task msg: {response_json["errors"][0]["message"]}')
                if isinstance(response_json, list) and "errors" in response_json[0]:
                    raise InvalidProtocol(
                        f'verify_task msg: {response_json[0]["errors"][0]["message"]}'
                    )

                if response_json[0].get("data"):
                    response_json = (
                        response_json[0].get("data", {}).get("campaignTaskMoveToVerificationV2")
                    )

                return response_json
        except InvalidProtocol as e:
            if "Campaign is not active" in str(e):  
                self.warning(f"Campaign is inactive. Skipping verification for task_config_id: {task_config_id}")
                return None
            else: 
                self.error(f"Verification failed: {e}")
                return None 
        except Exception as e:
            self.error(f"An error occurred during verification: {e}") 
            return None 

    async def complete_task(self, http_client: aiohttp.ClientSession, user_task_id: str):
        try:
            status = []
            json_data = [
                {
                    "operationName": OperationName.CampaignTaskMarkAsCompleted,
                    "variables": {
                        "userTaskId": user_task_id,
                    },
                    "query": Query.CampaignTaskMarkAsCompleted,
                }
            ]

            response = await http_client.post(url=self.GRAPHQL_URL, json=json_data)
            response.raise_for_status()

            response_json = await response.json()

            if "errors" in response_json:
                raise InvalidProtocol(f'verify_task msg: {response_json["errors"][0]["message"]}')

            if response_json[0].get("data"):
                response_json_data = (
                    response_json[0].get("data", {}).get("campaignTaskMarkAsCompleted", [])
                )
                if response_json_data.get("status") == "Completed":
                    status = True
            return status
        except Exception:
            return False

    async def check_proxy(self, http_client: aiohttp.ClientSession, proxy: Proxy) -> None:
        try:
            response = await http_client.get(
                url="http://httpbin.org/ip", timeout=aiohttp.ClientTimeout(5)
            )
            ip = (await response.json()).get("origin")
            self.info(f"Bind with proxy IP: <lc>{ip}</lc>")
        except Exception as error:
            self.error(f"Proxy: {proxy} | Error: {error}")

    async def run(self, proxy: str | None) -> None:
        if "tgWebAppData" in self.query_id:
            init_data = unquote(
                string=self.query_id.split("tgWebAppData=", maxsplit=1)[1].split(
                    "&tgWebAppVersion", maxsplit=1
                )[0]
            )
        else:
            init_data = self.query_id

        turbo_time = 0
        active_turbo = False
        ends_at_logged_time = 0

        ssl_context = TLSv1_3_BYPASS.create_ssl_context()

        conn = (
            ProxyConnector().from_url(url=proxy, rdns=True, ssl=ssl_context)
            if proxy
            else aiohttp.TCPConnector(ssl=ssl_context)
        )

        tele_user_obj = get_tele_user_obj_from_query_id(init_data)
        self.session_name = tele_user_obj.get("username")
        http_client = CloudflareScraper(headers=self.headers, connector=conn)
        if proxy:
            await self.check_proxy(http_client=http_client, proxy=proxy)
        http_client.headers["User-Agent"] = self.check_user_agent()
        while True:
            try:
                if time() - self.access_token_created_time >= 3000:
                    http_client.headers.pop("Authorization", None)
                    tg_web_data_cust = unquote(init_data)
                    user_obj = json.loads(
                        tg_web_data_cust.split("user=", maxsplit=1)[1].split(
                            "&auth_date", maxsplit=1
                        )[0]
                    )
                    user_id = user_obj.get("id")
                    first_name = user_obj.get("first_name")
                    last_name = user_obj.get("last_name")
                    username = user_obj.get("username")

                    query_id = tg_web_data_cust.split("query_id=", maxsplit=1)[1].split(
                        "&user", maxsplit=1
                    )[0]
                    user_data = tg_web_data_cust.split("user=", maxsplit=1)[1].split(
                        "&auth_date", maxsplit=1
                    )[0]
                    auth_date = tg_web_data_cust.split("auth_date=", maxsplit=1)[1].split(
                        "&hash", maxsplit=1
                    )[0]
                    hash_ = tg_web_data_cust.split("hash=", maxsplit=1)[1]

                    json_data = {
                        "operationName": OperationName.MutationTelegramUserLogin,
                        "query": Query.MutationTelegramUserLogin,
                        "variables": {
                            "webAppData": {
                                "auth_date": int(auth_date),
                                "hash": hash_, "query_id": query_id,
                                "checkDataString": f"auth_date={auth_date}\nquery_id={query_id}\nuser={user_data}",
                                "user": {
                                    "id": user_id,
                                    "allows_write_to_pm": True,
                                    "first_name": first_name,
                                    "last_name": last_name,
                                    "username": username,
                                    "language_code": "en",
                                },
                            },
                        },
                    }
                    access_token = await self.get_access_token(
                        http_client=http_client, tg_web_data=json_data
                    )

                    if not access_token:
                        continue

                    http_client.headers["Authorization"] = f"Bearer {access_token}"

                    self.access_token_created_time = time()

                    profile_data = await self.get_profile_data(http_client=http_client)

                    balance = profile_data["coinsAmount"]

                    nonce = profile_data["nonce"]

                    current_boss = profile_data["currentBoss"]
                    current_boss_level = current_boss["level"]
                    boss_max_health = current_boss["maxHealth"]
                    boss_current_health = current_boss["currentHealth"]

                    self.info(
                        f"Balance: <lc>{balance:,}</lc> | Boss level: <m>{current_boss_level}</m> | "
                        f"Boss health: <e>{boss_current_health:,}</e> / <r>{boss_max_health:,}</r>"
                    )

                    await asyncio.sleep(delay=1.5)

                    if settings.AUTO_PLAY_SPIN.lower() == "true":
                        spins = profile_data.get("spinEnergyTotal", 0)
                        while spins > 0:
                            await asyncio.sleep(delay=1)

                            spin_multiplier = calculate_spin_multiplier(spins=spins)
                            play_data = await self.play_slotmachine(
                                http_client=http_client, spin_multiplier=spin_multiplier
                            )

                            reward_amount = play_data.get("spinResults", [{}])[0].get(
                                "rewardAmount", 0
                            )
                            reward_type = play_data.get("spinResults", [{}])[0].get(
                                "rewardType", "NO"
                            )
                            spins = play_data.get("gameConfig", {}).get("spinEnergyTotal", 0)
                            balance = play_data.get("gameConfig", {}).get("coinsAmount", 0)

                            self.info(
                                f"Successfully played in slot machine | "
                                f"Balance: <lc>{balance:,}</lc> (<lg>+{reward_amount:,}</lg> <lm>{reward_type}</lm>) | "
                                f"Spins: <le>{spins:,}</le> (<lr>-{spin_multiplier:,}</lr>)"
                            )

                            await asyncio.sleep(delay=1)

                    taps = random.randint(
                        a=settings.RANDOM_TAPS_COUNT[0], b=settings.RANDOM_TAPS_COUNT[1]
                    )

                    available_energy = profile_data.get("currentEnergy", 0)
                    need_energy = taps * profile_data.get("weaponLevel", 0)

                    if active_turbo:
                        taps += settings.ADD_TAPS_ON_TURBO
                        need_energy = 0
                        if time() - turbo_time > 10:
                            active_turbo = False
                            turbo_time = 0

                    if need_energy > available_energy:
                        self.warning(
                            f"Need more energy: <ly>{available_energy:,}</ly>"
                            f"<lw>/</lw><le>{need_energy:,}</le> for <lg>{taps:,}</lg> taps"
                        )

                        sleep_between_clicks = random.randint(
                            a=settings.SLEEP_BETWEEN_TAP[0], b=settings.SLEEP_BETWEEN_TAP[1]
                        )

                        logger.info(f"Sleep <lw>{sleep_between_clicks:,}</lw>s")
                        await asyncio.sleep(delay=sleep_between_clicks)

                        profile_data = await self.get_profile_data(http_client=http_client)

                        continue

                    profile_data = await self.send_taps(
                        http_client=http_client, nonce=nonce, taps=taps
                    )

                    if not profile_data:
                        continue

                    available_energy = profile_data.get("currentEnergy", 0)
                    new_balance = profile_data.get("coinsAmount", 0)
                    calc_taps = new_balance - balance
                    balance = new_balance

                    free_boosts = profile_data.get("freeBoosts", {})
                    turbo_boost_count = free_boosts.get("currentTurboAmount", 0)
                    energy_boost_count = free_boosts.get("currentRefillEnergyAmount", 0)

                    next_tap_level = profile_data.get("weaponLevel", 0) + 1
                    next_energy_level = profile_data.get("energyLimitLevel", 0) + 1
                    next_charge_level = profile_data.get("energyRechargeLevel", 0) + 1

                    nonce = profile_data.get("nonce", "")

                    current_boss = profile_data.get("currentBoss", {})
                    current_boss_level = current_boss.get("level", 0)
                    boss_current_health = current_boss.get("currentHealth", 0)

                    self.success(
                        "Successfully tapped! | "
                        f"Balance: <lc>{balance:,}</lc> (<lg>+{calc_taps}</lg>) | "
                        f"Boss health: <lr>{boss_current_health:,}</lr> | "
                        f"Energy: <ly>{available_energy:,}</ly>"
                    )

                    if boss_current_health <= 0:
                        self.info(f"Setting next boss: <lm>{current_boss_level + 1}</lm> lvl")

                        status = await self.set_next_boss(http_client=http_client)
                        if status is True:
                            self.success(
                                f"Successfully setting next boss: "
                                f"<lm>{current_boss_level + 1}</lm>"
                            )

                        continue

                    if active_turbo is False:
                        if (
                            energy_boost_count > 0
                            and available_energy < settings.MIN_AVAILABLE_ENERGY
                            and settings.APPLY_DAILY_ENERGY.lower() == "true"
                        ):
                            self.info(
                                f"Sleep <ly>{format_duration(5)}</ly> before activating daily energy boost"
                            )
                            await asyncio.sleep(delay=5)

                            status = await self.apply_boost(
                                http_client=http_client, boost_type=FreeBoostType.ENERGY
                            )
                            if status is True:
                                self.success(f"Energy boost applied")

                                await asyncio.sleep(delay=1)

                            continue

                        if turbo_boost_count > 0 and settings.APPLY_DAILY_TURBO.lower() == "true":
                            self.info(
                                f"Sleep <ly>{format_duration(5)}</ly> before activating daily turbo boost"
                            )
                            await asyncio.sleep(delay=5)

                            status = await self.apply_boost(
                                http_client=http_client, boost_type=FreeBoostType.TURBO
                            )
                            if status is True:
                                self.success(f"Turbo boost applied")

                                await asyncio.sleep(delay=1)

                                active_turbo = True
                                turbo_time = time()

                            continue

                        if settings.USE_TAP_BOT.lower() == "true":
                            bot_config = await self.get_bot_config(http_client=http_client)

                            is_purchased = bot_config.get("isPurchased", False)
                            ends_at = bot_config.get("endsAt", None)

                            if not ends_at:
                                if is_purchased:
                                    await self.start_tapbot(http_client, bot_config)
                                else:
                                    await self.purchase_and_start_tapbot(http_client, bot_config)
                            else:
                                ends_at_date = datetime.strptime(ends_at, "%Y-%m-%dT%H:%M:%S.%f%z")
                                custom_ends_at_date = ends_at_date.strftime("%d.%m.%Y %H:%M:%S")
                                ends_at_timestamp = ends_at_date.timestamp()

                                if ends_at_logged_time <= time():
                                    self.info(f"TapBot ends at: <ly>{custom_ends_at_date}</ly>")
                                    ends_at_logged_time = time() + 900

                                if ends_at_timestamp < time():
                                    self.info(
                                        f"Sleep <ly>{format_duration(5)}</ly> before claim TapBot"
                                    )
                                    await asyncio.sleep(5)

                                    claim_data = await self.claim_bot(http_client=http_client)
                                    if claim_data:
                                        self.success(f"Successfully claimed TapBot")
                                        await self.start_tapbot(http_client, bot_config)
                                elif not is_purchased:
                                    await self.purchase_and_start_tapbot(http_client, bot_config)

                        if (
                            settings.AUTO_UPGRADE_TAP.lower() == "true"
                            and next_tap_level <= settings.MAX_TAP_LEVEL
                        ):
                            need_balance = 1000 * (2 ** (next_tap_level - 1))

                            if balance > need_balance:
                                status = await self.upgrade_boost(
                                    http_client=http_client, boost_type=UpgradableBoostType.TAP
                                )
                                if status is True:
                                    self.success(f"Tap upgraded to <lm>{next_tap_level}</lm> lvl")

                                    await asyncio.sleep(delay=1)
                            else:
                                self.warning(
                                    f"Need more gold for upgrade tap to <lm>{next_tap_level}</lm> lvl "
                                    f"(<lc>{balance}</lc><lw>/</lw><le>{need_balance}</le>)"
                                )

                        if (
                            settings.AUTO_UPGRADE_ENERGY.lower() == "true"
                            and next_energy_level <= settings.MAX_ENERGY_LEVEL
                        ):
                            need_balance = 1000 * (2 ** (next_energy_level - 1))
                            if balance > need_balance:
                                status = await self.upgrade_boost(
                                    http_client=http_client, boost_type=UpgradableBoostType.ENERGY
                                )
                                if status is True:
                                    self.success(
                                        f"Energy upgraded to <lm>{next_energy_level}</lm> lvl"
                                    )

                                    await asyncio.sleep(delay=1)
                            else:
                                self.warning(
                                    f"Need more gold for upgrade energy to <lm>{next_energy_level}</lm> lvl "
                                    f"(<lc>{balance}</lc><lw>/</lw><le>{need_balance}</le>)"
                                )

                        if (
                            settings.AUTO_UPGRADE_CHARGE.lower() == "true"
                            and next_charge_level <= settings.MAX_CHARGE_LEVEL
                        ):
                            need_balance = 1000 * (2 ** (next_charge_level - 1))

                            if balance > need_balance:
                                status = await self.upgrade_boost(
                                    http_client=http_client, boost_type=UpgradableBoostType.CHARGE
                                )
                                if status is True:
                                    self.success(
                                        f"Charge upgraded to <lm>{next_charge_level}</lm> lvl"
                                    )

                                    await asyncio.sleep(delay=1)
                            else:
                                self.warning(
                                    f"Need more gold for upgrade charge to <lm>{next_energy_level}</lm> lvl "
                                    f"(<lc>{balance}</lc><lw>/</lw><le>{need_balance}</le>)"
                                )

                        if settings.AUTO_CLEAR_MISSION.lower() == "true":
                            campaign_list_resp = await self.get_campaign_list(
                                http_client=http_client
                            )
                            if campaign_list_resp:
                                if campaign_list_resp[0].get("data"):
                                    special_campaign_data = (
                                        campaign_list_resp[0]
                                        .get("data", {})
                                        .get("campaignLists", {})
                                        .get("special", [])
                                    )
                                    normal_campaign_data = (
                                        campaign_list_resp[0]
                                        .get("data", {})
                                        .get("campaignLists", {})
                                        .get("normal", [])
                                    )
                                    for s_c in special_campaign_data:
                                        parent_c_id = s_c.get("id")
                                        task_list = await self.get_campaign_task_list(
                                            http_client=http_client, campaign_id=parent_c_id
                                        )
                                        for task in task_list:
                                            if task.get("status") == "Completed":
                                                continue
                                            task_id = task.get("id")
                                            task_detail = await self.get_task_by_id(
                                                http_client=http_client, task_id=task_id
                                            )
                                            if task_detail:
                                                complete_delay_sec = 1
                                                task_name = task_detail.get("name")
                                                task_detail_id = task_detail.get("id")
                                                task_status = task.get("status")
                                                if task_status == "Verification":
                                                    user_task_id = task_detail.get("userTaskId")
                                                    complete_available_ts = task_detail.get(
                                                        "verificationAvailableAt"
                                                    )
                                                    complete_delay_sec = check_complete_task_delay(
                                                        complete_available_ts
                                                    )
                                                    if complete_delay_sec < 0:
                                                        complete_delay_sec = 1
                                                    await asyncio.sleep(delay=complete_delay_sec)
                                                    task_completed = await self.complete_task(
                                                        http_client=http_client,
                                                        user_task_id=user_task_id,
                                                    )
                                                    if task_completed:
                                                        self.success(
                                                            f"Successfully completed <lc>{task_name}</lc> quest"
                                                        )
                                                else:
                                                    verification_detail = await self.verify_task(
                                                        http_client=http_client,
                                                        task_config_id=task_detail_id,
                                                    )
                                                    if verification_detail:
                                                        self.success(
                                                            f"Successfully started <lc>{task_name}</lc> quest"
                                                        )
                                                    await asyncio.sleep(delay=4)

                                    for n_c in normal_campaign_data:
                                        parent_n_id = n_c.get("id")
                                        task_list = await self.get_campaign_task_list(
                                            http_client=http_client, campaign_id=parent_n_id
                                        )
                                        for task in task_list:
                                            if task.get("status") == "Completed":
                                                continue
                                            task_id = task.get("id")
                                            task_detail = await self.get_task_by_id(
                                                http_client=http_client, task_id=task_id
                                            )
                                            if task_detail:
                                                complete_delay_sec = 1
                                                task_name = task_detail.get("name")
                                                task_detail_id = task_detail.get("id")
                                                task_status = task.get("status")
                                                if task_status == "Verification":
                                                    user_task_id = task_detail.get("userTaskId")
                                                    complete_available_ts = task_detail.get(
                                                        "verificationAvailableAt"
                                                    )
                                                    complete_delay_sec = check_complete_task_delay(
                                                        complete_available_ts
                                                    )
                                                    if complete_delay_sec < 0:
                                                        complete_delay_sec = 1
                                                    await asyncio.sleep(delay=complete_delay_sec)
                                                    task_completed = await self.complete_task(
                                                        http_client=http_client,
                                                        user_task_id=user_task_id,
                                                    )
                                                    if task_completed:
                                                        self.success(
                                                            f"Successfully completed <lc>{task_name}</lc> quest"
                                                        )
                                                else:
                                                    verification_detail = await self.verify_task(
                                                        http_client=http_client,
                                                        task_config_id=task_detail_id,
                                                    )
                                                    if verification_detail:
                                                        self.success(
                                                            f"Successfully started <lc>{task_name}</lc> quest"
                                                        )
                                                    await asyncio.sleep(delay=4)

                        if available_energy < settings.MIN_AVAILABLE_ENERGY:
                            self.info(f"Minimum energy reached: <ly>{available_energy:,}</ly>")

                            if isinstance(settings.SLEEP_BY_MIN_ENERGY, list):
                                sleep_time = random.randint(
                                    a=settings.SLEEP_BY_MIN_ENERGY[0],
                                    b=settings.SLEEP_BY_MIN_ENERGY[1],
                                )
                            else:
                                sleep_time = settings.SLEEP_BY_MIN_ENERGY

                            self.info(f"Sleep <ly>{format_duration(sleep_time)}</ly>")
                            await asyncio.sleep(delay=sleep_time)

                await asyncio.sleep(delay=1)

                sleep_duration = random.randint(
                    a=settings.SLEEP_BETWEEN_TAP[0], b=settings.SLEEP_BETWEEN_TAP[1]
                )

                if active_turbo is True:
                    sleep_duration = settings.ACTIVE_TURBO_DELAY

                self.access_token_created_time = 0

                self.info(f"Delay <ly>{format_duration(sleep_duration)}</ly>")

                await asyncio.sleep(sleep_duration)

            except InvalidSessionException as error:
                raise error

            except ExpiredTokenException as error:
                self.warning(f"<ly>{error}</ly>")
                await asyncio.sleep(delay=60)
                continue

            except GameSessionNotFoundException as error:
                self.warning(f"<ly>{error}</ly>")
                await asyncio.sleep(delay=60)
                continue

            except ErrorStartGameException as error:
                self.warning(f"<ly>{error}</ly>")
                await asyncio.sleep(delay=60)
                continue

            except Exception as error:
                self.error(f"Unknown error: {error}")
                self.access_token_created_time = 0
                await asyncio.sleep(delay=60)


async def run_tapper(query_id: str, proxy: str | None):
    await Tapper(query_id=query_id).run(proxy=proxy)


async def main():
    try:
        await process()
    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())