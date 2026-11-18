"""Check that the Native App dashboard is served and renders the pipeline's data.

Usage: python bin/check_app.py <app URL printed by `make app`>

Fetches the page, then runs the Streamlit script once over the app's WebSocket,
the way a browser does, and checks what it renders: no exceptions or error
messages, the overview metrics, and the Plotly charts. Exits non-zero otherwise.
"""

import asyncio
import ssl
import sys
import time

import requests
import urllib3
from google.protobuf.json_format import MessageToDict
from streamlit.proto.BackMsg_pb2 import BackMsg
from streamlit.proto.ForwardMsg_pb2 import ForwardMsg
from websockets.asyncio.client import connect

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

METRICS = ["Total Machines", "Healthy Machines", "Critical Machines", "Average Risk Score"]
# The app's process starts on the first request and may install its packages first.
PAGE_TIMEOUT = 300
SCRIPT_TIMEOUT = 120


def fetch_page(url):
    """Return the app's final URL once its page is served, following redirects."""
    deadline = time.monotonic() + PAGE_TIMEOUT
    while True:
        response = requests.get(url, verify=False, timeout=PAGE_TIMEOUT)
        if response.ok and "<title>Streamlit</title>" in response.text:
            return response.url
        if time.monotonic() > deadline:
            sys.exit(f"App page not served: HTTP {response.status_code}\n{response.text[:500]}")
        time.sleep(5)


async def run_script(url):
    """Run the app's script once and return the elements it renders."""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    stream = url.replace("http", "ws", 1).rstrip("/") + "/_stcore/stream"
    elements = []
    async with connect(
        stream,
        subprotocols=["streamlit", "PLACEHOLDER_AUTH_TOKEN"],
        ssl=ssl_context if stream.startswith("wss://") else None,
        max_size=None,
    ) as ws:
        rerun = BackMsg()
        rerun.rerun_script.SetInParent()
        await ws.send(rerun.SerializeToString())
        while True:
            msg = ForwardMsg()
            msg.ParseFromString(await ws.recv())
            kind = msg.WhichOneof("type")
            if kind == "delta" and msg.delta.WhichOneof("type") == "new_element":
                elements.append(MessageToDict(msg.delta.new_element))
            elif kind == "script_finished":
                status = ForwardMsg.ScriptFinishedStatus.Name(msg.script_finished)
                if status != "FINISHED_SUCCESSFULLY":
                    sys.exit(f"Script did not finish successfully: {status}")
                return elements


def main(url):
    url = fetch_page(url)
    print(f"Page served at {url}")
    elements = asyncio.run(asyncio.wait_for(run_script(url), SCRIPT_TIMEOUT))

    problems = [e for e in elements if "exception" in e]
    problems += [e for e in elements if e.get("alert", {}).get("format") in ["ERROR", "WARNING"]]
    if problems:
        sys.exit(f"The dashboard reported errors: {problems}")

    metrics = {e["metric"]["label"]: e["metric"]["body"] for e in elements if "metric" in e}
    missing = [name for name in METRICS if name not in metrics]
    if missing:
        sys.exit(f"Missing metrics {missing}; rendered metrics: {metrics}")
    charts = sum("plotlyChart" in e for e in elements)
    if not charts:
        sys.exit("The dashboard rendered no charts")
    if int(metrics["Total Machines"]) < 1:
        sys.exit(f"The dashboard shows no machines: {metrics}")

    for name in METRICS:
        print(f"  {name}: {metrics[name]}")
    print(f"  Charts: {charts}")
    print("Dashboard OK")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
