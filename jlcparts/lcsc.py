import json
import requests
import os
import time
import random
import string
import urllib
import hashlib
from requests.exceptions import ConnectionError

LCSC_KEY = os.environ.get("LCSC_KEY")
LCSC_SECRET = os.environ.get("LCSC_SECRET")

class RateLimitError(Exception):
    """Raised when LCSC API triggers rate limiting (HTTP 429, 403, or WAF challenge)."""
    pass

class LcscApiError(Exception):
    """Raised when LCSC API returns an unexpected error."""
    pass

def fetchLcscProductDetail(lcscNumber, session=None, timeout=10, max_retries=3, backoff_base=2.0):
    """
    Fetch component details and image URLs from LCSC public API.
    Does not require any API keys or secrets.
    Handles HTTP status code checking, rate limiting detection, and exponential backoff.
    """
    code_str = str(lcscNumber).strip()
    if not code_str.upper().startswith("C"):
        code_str = f"C{code_str}"
    else:
        code_str = code_str.upper()

    url = f"https://wmsc.lcsc.com/ftps/wm/product/detail?productCode={code_str}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    }
    client = session or requests

    last_error = None
    for attempt in range(max_retries + 1):
        try:
            resp = client.get(url, headers=headers, timeout=timeout)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e
            if attempt < max_retries:
                sleep_time = (backoff_base ** attempt) + random.uniform(0.5, 1.5)
                time.sleep(sleep_time)
                continue
            raise LcscApiError(f"Network error fetching {code_str}: {e}") from e

        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as e:
                # Returned 200 but not JSON (e.g. HTML challenge/captcha page)
                if attempt < max_retries:
                    sleep_time = (backoff_base ** attempt) + random.uniform(1.0, 2.0)
                    time.sleep(sleep_time)
                    continue
                raise RateLimitError(f"Received non-JSON response (WAF challenge) for {code_str}") from e

            code = data.get("code")
            if code == 200:
                return data.get("result")
            elif code in (429, 403):
                if attempt < max_retries:
                    sleep_time = (backoff_base ** attempt) * 2 + random.uniform(1.0, 3.0)
                    time.sleep(sleep_time)
                    continue
                raise RateLimitError(f"LCSC business code rate limit: {data.get('msg') or code} for {code_str}")
            else:
                return None

        elif resp.status_code in (429, 403):
            last_error = resp.status_code
            if attempt < max_retries:
                # Exponential backoff with random jitter: 2s, 4s, 8s...
                sleep_time = (backoff_base ** (attempt + 1)) + random.uniform(1.0, 3.0)
                time.sleep(sleep_time)
                continue
            raise RateLimitError(f"HTTP {resp.status_code} WAF/Rate limit triggered for {code_str}")

        elif resp.status_code in (500, 502, 503, 504):
            last_error = resp.status_code
            if attempt < max_retries:
                sleep_time = (backoff_base ** attempt) + random.uniform(0.5, 1.5)
                time.sleep(sleep_time)
                continue
            raise LcscApiError(f"HTTP {resp.status_code} server error for {code_str}")

        elif resp.status_code == 404:
            return None
        else:
            raise LcscApiError(f"HTTP {resp.status_code} unexpected response for {code_str}")

    if last_error:
        raise RateLimitError(f"Retries exhausted for {code_str}: {last_error}")
    return None

def makeLcscRequest(url, payload=None):
    if payload is None:
        payload = {}
    payload = [(key, value) for key, value in payload.items()]
    payload.sort(key=lambda x: x[0])
    newPayload = {
        "key": LCSC_KEY,
        "nonce": "".join(random.choices(string.ascii_lowercase, k=16)),
        "secret": LCSC_SECRET,
        "timestamp": str(int(time.time())),
    }
    for k, v in payload:
        newPayload[k] = v
    payloadStr = urllib.parse.urlencode(newPayload).encode("utf-8")
    newPayload["signature"] = hashlib.sha1(payloadStr).hexdigest()

    return requests.get(url, params=newPayload)

def pullPreferredComponents():
    try:
        resp = requests.get("https://jlcpcb.com/api/overseas-pcb-order/v1/getAll", timeout=10)
        cookies = resp.cookies.get_dict()
        token = cookies.get("XSRF-TOKEN")
        if not token:
            return set()

        headers = {
            "Content-Type": "application/json",
            "X-XSRF-TOKEN": token,
        }
        PAGE_SIZE = 1000

        currentPage = 1
        components = set()
        while True:
            body = {
                "currentPage": currentPage,
                "pageSize": PAGE_SIZE,
                "preferredComponentFlag": True
            }

            resp = requests.post(
                "https://jlcpcb.com/api/overseas-pcb-order/v1/shoppingCart/smtGood/selectSmtComponentList",
                headers=headers,
                json=body,
                timeout=10
            )

            body = resp.json()
            for c in [x["componentCode"] for x in body["data"]["componentPageInfo"]["list"]]:
                components.add(c)

            if not body["data"]["componentPageInfo"]["hasNextPage"]:
                break
            currentPage += 1

        return components
    except Exception as e:
        print(f"Warning: Failed to pull preferred components: {e}")
        return set()

if __name__ == "__main__":
    r = makeLcscRequest("https://ips.lcsc.com/rest/wmsc2agent/product/info/C7063")
    print(r.json())

