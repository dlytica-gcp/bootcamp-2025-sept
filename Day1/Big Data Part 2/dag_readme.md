# Airflow DAG: NiFi ELT Orchestrator

This DAG (`api_to_postgres_nifi`) is an **Airflow workflow** that controls an **Apache NiFi flow** remotely using NiFi’s REST API.  

It ensures:
1. **NiFi is running and reachable**  
2. **Gets an access token** (for authentication)  
3. **Starts a specific Process Group (PG)** in NiFi  
4. **Waits until the flow finishes processing data** (based on conditions like file count or time)  
5. **Stops the Process Group** (so processors don’t run forever)  
6. **Cleans up leftover queues** (purges all flowfiles still waiting in NiFi connections)  

---

## 🟠 Key Concepts (For Beginners)

- **DAG** = Directed Acyclic Graph = a pipeline of tasks in Airflow.  
- **Task** = one unit of work (e.g., start NiFi, wait, stop NiFi).  
- **NiFi Process Group (PG)** = a box in NiFi canvas containing processors and connections.  
- **FlowFile Queue** = temporary buffer between processors in NiFi.  
- **Access Token** = password-like key used to call NiFi APIs securely.  

---

## 🔵 Step-by-Step Explanation of the DAG

### 1. `test_nifi_connectivity`
- Pings NiFi until it responds (`/system-diagnostics`).  
- Makes sure NiFi is **up before continuing**.  
- Returns **True/False**.

---

### 2. `get_nifi_access_token`
- Logs in to NiFi using **username/password** stored in Airflow connection (`nifi_default`).  
- Calls `/access/token` to get a **Bearer Token**.  
- Retries 5 times with exponential backoff if NiFi is still starting.  
- Returns **token string**.

---

### 3. `start_nifi_flow`
- Starts the Process Group (PG) using its **ID**.  
- API call: `PUT /flow/process-groups/{pg_id}` with `"state": "RUNNING"`.  
- Now all processors inside that PG begin running.  
- Returns **status success**.

---

### 4. `wait_for_nifi_flow_completion`
- Keeps checking `/status` of the PG.  
- Looks at:
  - **flowFilesOut** → how many files went through  
  - **flowFilesQueued** → how many are waiting  
- Stops when:
  1. At least `STOP_AFTER_FLOWFILES` (100) are produced  
  2. OR `MAX_RUN_SECONDS` (10s) are reached  
  3. OR no files queued + output stable  

---

### 5. `stop_nifi_flow`
- Stops the PG after completion (or if something fails).  
- Trigger rule is **all_done**, meaning it runs **even if previous task failed**.  
- API call: `PUT /flow/process-groups/{pg_id}` with `"state": "STOPPED"`.  

---

### 6. `purge_nifi_queues`
- Cleans all queues (connections) inside the PG.  
- Steps:
  - Finds all **connection IDs**  
  - Calls `/drop-requests` to clear queued FlowFiles  
  - Waits until each drop finishes  
- Final check: queued count must be **0**.  

---

## 🟣 Task Dependency Flow

- First: **Check NiFi is up**  
- Then: **Get token**  
- Then: **Start PG**  
- Then: **Wait for completion**  
- Then: **Stop PG**  
- Finally: **Purge queues**  

---

## 🟢 Flowchart of the DAG

```mermaid
flowchart TD

    A[Start DAG: api_to_postgres_nifi] --> B[Test NiFi Connectivity]
    B --> C[Get NiFi Access Token]
    C --> D[Start NiFi Flow (PG RUNNING)]
    D --> E[Wait for NiFi Flow Completion]

    E --> F[Stop NiFi Flow (PG STOPPED)]
    E --> F
    F --> G[Purge NiFi Queues]

    G --> H[End DAG]
