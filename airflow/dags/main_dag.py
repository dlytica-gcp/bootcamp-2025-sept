import json
import urllib3
from pendulum import datetime
from airflow.decorators import task, dag
import requests
import time
from airflow.hooks.base import BaseHook

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

'''
Global configs for storing IDs of NiFi Process Groups (PGs) to control.
Pulling NiFi credentials from .env file.
Configuring NiFi Base URL and SSL verification path.
'''
NIFI_PG_IDS = {
    "pg": "134eb35b-0199-1000-9b35-a2b3144a420e",
}

NIFI_BASE_URL = "https://nifi:8443"
REQUESTS_VERIFY = "/etc/ssl/certs/nifi-ca.pem"  # mounted in Airflow containers

'''
Stop criteria for the NiFi flow run:
- STOP_AFTER_FLOWFILES: stop after this many new flowfiles have been produced
- MAX_RUN_SECONDS: or stop after this many seconds
- STATUS_POLL_SECONDS: how often to poll the PG status
'''
STOP_AFTER_FLOWFILES = 100
MAX_RUN_SECONDS = 10
STATUS_POLL_SECONDS = 5


def get_nifi_session():
    '''
    Creates a reusable requests.Session with SSL verification (points to mounted CA cert).
    Custom User-Agent header.
    '''
    s = requests.Session()
    s.verify = REQUESTS_VERIFY
    s.headers.update({"User-Agent": "Airflow-NiFi-Client/1.0"})
    return s


def get_nifi_credentials():
    '''
    Retrieves NiFi credentials from Airflow Connections (if set up).
    Falls back to hardcoded values if not found.

    Credentials should be stored in Airflow Connection with Conn ID "nifi_default".
    Connection type should be "HTTP".
    Host: https://nifi:8443
    Login: <username>
    Password: <password>
    '''
    try:
        conn = BaseHook.get_connection("nifi_default")
        username = conn.login
        password = conn.password
        return username, password
    except Exception as e:
        print(f"Could not get NiFi credentials from Airflow Connections: {e}")


def wait_until_ready(base_url: str, timeout_sec=180):
    '''
    Keeps pinging /nifi-api/system-diagnostics until a 200, 401, or 403 response is received.
    Retries until timeout_sec(180) is reached.
    Prevents race conditions if NiFi is starting up while Airflow DAG runs.
    '''
    s = get_nifi_session()
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            r = s.get(f"{base_url}/nifi-api/system-diagnostics", timeout=8)
            if r.status_code in (200, 401, 403):
                print(f"NiFi is up (status={r.status_code})")
                return True
            else:
                print(f"NiFi not ready yet (status={r.status_code})")
        except Exception as e:
            print(f"NiFi not ready yet: {e}")
        time.sleep(3)
    raise RuntimeError("NiFi did not become ready in time")


def _extract_connection_ids(flow_node):
    """
    Recursive helper to collect all queue (connection) IDs under a flow node.
    Looks for 'connections' and 'processGroups' keys.
    Returns a list of connection IDs found.
    """
    ids = []
    if not flow_node:
        return ids

    # connections in this level
    for conn in flow_node.get("connections", []):
        # IDs can be top-level or under 'component'
        cid = conn.get("id") or (conn.get("component") or {}).get("id")
        if cid:
            ids.append(cid)

    # recurse into child PGs
    for child_pg in flow_node.get("processGroups", []):
        child_flow = (child_pg.get("component") or {}).get("id")
        # In NiFi 2.x, we need to follow the "flow" key inside each child node
        # if present; the GET below provides the full tree already, so just
        # traverse the nested 'flow' if present on the child.
        inner = child_pg.get("flow") or child_pg.get("component") or {}
        ids.extend(_extract_connection_ids(inner))

    return ids


def _collect_all_connection_ids(session, token, pg_id):
    """
    Calls the NiFi API to collect all connection (queue) IDs under a given Process Group ID (pg_id),
    Returns full flow tree for the PG.
    Uses _extract_connection_ids to find all queues.
    If nothing found (schema mismatch), it falls back to: GET /nifi-api/process-groups/{pg_id}/connections.
    Deduplicates IDs.
    Returns all queue connection IDs under the PG (needed for purge).
    """
    r = session.get(
        f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    r.raise_for_status()
    tree = r.json()
    flow = (tree.get("processGroupFlow") or {}).get("flow") or {}
    conn_ids = _extract_connection_ids(flow)

    # Fallback: if recursion above did not find any due to schema shape, also try flat list
    if not conn_ids:
        r2 = session.get(
            f"{NIFI_BASE_URL}/nifi-api/process-groups/{pg_id}/connections",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if r2.status_code in (200, 201):
            for conn in r2.json().get("connections", []):
                cid = conn.get("id") or (conn.get("component") or {}).get("id")
                if cid:
                    conn_ids.append(cid)

    conn_ids = list(dict.fromkeys(conn_ids))  # de-dupe, preserve order
    print(f"Found {len(conn_ids)} connection(s) to purge under PG {pg_id}")
    return conn_ids


def _drop_queue_and_wait(session, token, conn_id, timeout_sec=120):
    """
    POST a drop request for a connection (queue), then poll until finished.
    """
    # Create drop request
    r = session.post(
        f"{NIFI_BASE_URL}/nifi-api/flowfile-queues/{conn_id}/drop-requests",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    if r.status_code not in (200, 201, 202):
        raise RuntimeError(
            f"Create drop request failed for {conn_id}: {r.status_code} {r.text}")

    entity = r.json() if r.text else {}
    dr = entity.get("dropRequest") or entity  # some versions wrap it
    dr_id = dr.get("id")
    if not dr_id:
        raise RuntimeError(
            f"No dropRequest id returned for {conn_id}: {entity}")

    # Poll status
    start = time.time()
    while True:
        s = session.get(
            f"{NIFI_BASE_URL}/nifi-api/flowfile-queues/{conn_id}/drop-requests/{dr_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if s.status_code not in (200, 201):
            print(
                f"Drop status check failed for {conn_id}: {s.status_code} {s.text}")
            time.sleep(2)
            continue

        body = s.json() if s.text else {}
        body_dr = body.get("dropRequest") or body

        finished = bool(body_dr.get("finished"))
        dropped = body_dr.get("droppedCount") or body_dr.get("currentCount")
        percent = body_dr.get("percentCompleted")

        print(
            f"[drop {conn_id}] finished={finished} dropped={dropped} percent={percent}")

        if finished:
            break

        if time.time() - start > timeout_sec:
            raise TimeoutError(
                f"Dropping queue {conn_id} did not finish within {timeout_sec}s")
        time.sleep(2)

    # Clean up the drop request (optional)
    session.delete(
        f"{NIFI_BASE_URL}/nifi-api/flowfile-queues/{conn_id}/drop-requests/{dr_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )


'''
- Defines Airflow DAG with ID api_to_postgres_nifi.
- No schedule (manual trigger).
- Tags it as nifi, elt.
- Returns the workflow instance.
'''


@dag(
    dag_id="api_to_postgres_nifi",
    start_date=datetime(2025, 6, 23, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["nifi", "elt", "airflow"],
    render_template_as_native_obj=True,
)
def nifi_elt_orchestrator():

    @task
    def test_nifi_connectivity():
        '''
        Optional task to test connectivity to NiFi before proceeding.
        Calls wait_until_ready to confirm NiFi is reachable.
        Returns True if up, else False.
        Useful as a sanity check before trying anything.
        '''
        try:
            wait_until_ready(NIFI_BASE_URL, timeout_sec=180)
            return True
        except Exception as e:
            print(f"Connectivity test failed: {e}")
            return False

    @task
    def get_nifi_access_token():
        '''
        Requests a bearer token for Single User Auth via: POST /nifi-api/access/token, with form data {username, password}.
        Retries up to 5 times with exponential backoff.
        Returns token string if successful.
        '''
        username, password = get_nifi_credentials()
        wait_until_ready(NIFI_BASE_URL, timeout_sec=180)

        session = get_nifi_session()
        attempts, backoff = 5, 2
        for i in range(1, attempts + 1):
            try:
                print(f"[Attempt {i}] Getting token from {NIFI_BASE_URL}")
                r = session.post(
                    f"{NIFI_BASE_URL}/nifi-api/access/token",
                    data={
                        "username": username,
                        "password": password
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=30,
                )
                print(f"Token request status code: {r.status_code}")
                if r.status_code in (200, 201):
                    token = r.text.strip()
                    print(f"Got token (len={len(token)})")
                    return token
                else:
                    print(
                        f"Token retrieval failed: {r.status_code} - {r.text}")
            except Exception as e:
                print(f"Token request error: {e}")

            if i < attempts:
                sleep_for = backoff ** i
                print(f"Retrying in {sleep_for}s...")
                time.sleep(sleep_for)

        raise Exception("Failed to obtain NiFi access token after retries")

    @task
    def start_nifi_flow(token: str):
        '''
        Sends: PUT /nifi-api/flow/process-groups/{pg_id}{ "id": pg_id, "state": "RUNNING" }
        This starts the PG (all processors inside start running).
        Returns status JSON with pg_id.
        '''
        if not token:
            raise Exception("No token provided")
        pg_id = list(NIFI_PG_IDS.values())[0]
        print(f"Starting flow for PG: {pg_id}")
        session = get_nifi_session()
        r = session.put(
            f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"id": pg_id, "state": "RUNNING"},
            timeout=30,
        )
        print(f"Start flow status: {r.status_code} body: {r.text}")
        if r.status_code not in (200, 201):
            raise Exception(f"Start flow failed: {r.status_code} - {r.text}")
        return {"status": "success", "pg_id": pg_id}

    @task
    def wait_for_nifi_flow_completion(token: str):
        '''
        Monitors PG status until stop criteria are met.
        Calls: GET /nifi-api/flow/process-groups/{pg_id}/status
        Extracts:
        flowFilesOut (how many flowfiles have exited).
        flowFilesQueued (how many waiting in queues).

        Keeps track of:
        Baseline output when flow starts.
        New outputs produced in this run (produced_now).

        Stop conditions:
        At least STOP_AFTER_FLOWFILES files produced.
        Time > MAX_RUN_SECONDS.
        Flow idle (no queued files, output stable).
        '''
        if not token:
            raise Exception("No token provided")

        pg_id = list(NIFI_PG_IDS.values())[0]
        session = get_nifi_session()

        start_ts = time.time()
        baseline_out = None
        last_out = 0

        while True:
            r = session.get(
                f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}/status",
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            if r.status_code not in (200, 201):
                print(f"Status check failed: {r.status_code} - {r.text}")
                time.sleep(STATUS_POLL_SECONDS)
                continue

            snap = (
                r.json()
                .get("processGroupStatus", {})
                .get("aggregateSnapshot", {})
            )

            flowfiles_out = int(snap.get("flowFilesOut", 0))
            flowfiles_queued = int(snap.get("flowFilesQueued", 0))

            # Count only outputs produced during THIS run
            if baseline_out is None:
                baseline_out = flowfiles_out

            produced_now = flowfiles_out - baseline_out
            elapsed = int(time.time() - start_ts)

            print(
                f"[{elapsed}s] queued={flowfiles_queued} "
                f"out_total={flowfiles_out} out_new={produced_now}"
            )

            # --- Stop conditions ---
            if produced_now >= STOP_AFTER_FLOWFILES:
                print(
                    f"Reached {STOP_AFTER_FLOWFILES} new flow files. Stopping.")
                return {"status": "completed-by-count", "pg_id": pg_id, "produced": produced_now}

            if elapsed >= MAX_RUN_SECONDS:
                print(f"Reached time limit {MAX_RUN_SECONDS}s. Stopping.")
                return {"status": "completed-by-time", "pg_id": pg_id, "produced": produced_now}

            # Optional idle stop (only if something was produced)
            if flowfiles_queued == 0 and produced_now == last_out and produced_now > 0:
                print("No queue and output stable. Stopping.")
                return {"status": "completed-by-idle", "pg_id": pg_id, "produced": produced_now}

            last_out = produced_now
            time.sleep(STATUS_POLL_SECONDS)

    @task(trigger_rule="all_done")
    def stop_nifi_flow(token: str):
        '''
        Ensures PG is stopped after flow completes (or fails).
        Calls: PUT /nifi-api/flow/process-groups/{pg_id} { "id": pg_id, "state": "STOPPED" }
        Uses trigger_rule="all_done" so it always runs (even if upstream failed).
        '''
        pg_id = list(NIFI_PG_IDS.values())[0]
        if not token:
            print("No token available, skipping stop")
            return {"status": "skipped", "pg_id": pg_id}

        session = get_nifi_session()
        r = session.put(
            f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"id": pg_id, "state": "STOPPED"},
            timeout=30,
        )
        print(f"Stop flow status: {r.status_code} body: {r.text}")
        return {
            "status": "stopped" if r.status_code in (200, 201) else "failed",
            "pg_id": pg_id,
        }

    @task(trigger_rule="all_done")
    def purge_nifi_queues(token: str):
        '''
        Cleans out leftover data in all queues inside the PG.
        Steps:
        Uses _collect_all_connection_ids to find queue IDs.
        Calls _drop_queue_and_wait on each.
        Double-checks with GET /status that queued count = 0.
        Runs always (trigger rule = all_done).
        '''
        if not token:
            print("No token available, skipping purge")
            return {"status": "skipped"}

        pg_id = list(NIFI_PG_IDS.values())[0]
        session = get_nifi_session()

        conn_ids = _collect_all_connection_ids(session, token, pg_id)
        if not conn_ids:
            print("No connections found to purge.")
            return {"status": "nothing-to-purge", "pg_id": pg_id}

        for cid in conn_ids:
            try:
                _drop_queue_and_wait(session, token, cid, timeout_sec=180)
            except Exception as e:
                print(f"Queue purge failed on connection {cid}: {e}")
                # keep going to try other queues

        # Final sanity check: queued count should be 0 now
        r = session.get(
            f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}/status",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if r.status_code in (200, 201):
            snap = r.json().get("processGroupStatus", {}).get("aggregateSnapshot", {})
            q = int(snap.get("flowFilesQueued", 0))
            print(f"Post-purge queued count: {q}")

        return {"status": "purged", "pg_id": pg_id, "connections": len(conn_ids)}

    '''
    Task orchestration flow:
    - Check NiFi is up
    - Get token
    - Start PG
    - Wait until flow completes
    - Stop PG
    - Purge queues
    '''
    connectivity_test = test_nifi_connectivity()
    token_task = get_nifi_access_token()
    start_task = start_nifi_flow(token_task)
    wait_task = wait_for_nifi_flow_completion(token_task)
    stop_task = stop_nifi_flow(token_task)
    purge_task = purge_nifi_queues(token_task)

    connectivity_test >> token_task >> start_task >> wait_task >> stop_task >> purge_task


dag_instance = nifi_elt_orchestrator()
