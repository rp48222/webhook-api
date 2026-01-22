from fastapi import FastAPI, Body, HTTPException, Request, BackgroundTasks
from datetime import datetime
import uuid
import httpx
import time

from db import (
    init_db,
    create_delivery,
    get_delivery,
    update_delivery,
    list_deliveries_for_user,
    create_destination_db,
    list_destinations_db,
    get_active_destination_url
)

app = FastAPI()

@app.on_event("startup")
def on_startup():
    init_db()

# TEMP storage (we still keep user grouping in memory for now, but deliveries are in SQLite)
# Structure:
# USERS[user_id] = { "destinations": [...], "events": [...] }
USERS = {}

async def attempt_delivery(delivery_id: str, destination_url: str, event: dict):
    max_attempts = 3
    last_error = None

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                resp = await client.post(destination_url, json=event)

                # Consider 2xx as success
                if 200 <= resp.status_code < 300:
                    update_delivery(
                        delivery_id=delivery_id,
                        status="delivered",
                        attempts=attempt,
                        last_error=None
                    )
                    return resp.status_code

                # Non-2xx = failure worth retrying
                last_error = f"Non-2xx response: {resp.status_code}"
                update_delivery(
                    delivery_id=delivery_id,
                    status="pending",
                    attempts=attempt,
                    last_error=last_error
                )

            except Exception as e:
                last_error = str(e)
                update_delivery(
                    delivery_id=delivery_id,
                    status="pending",
                    attempts=attempt,
                    last_error=last_error
                )

            # Backoff before next attempt (1s, 2s, 4s)
            time.sleep(2 ** (attempt - 1))

    # If we get here, all attempts failed
    update_delivery(
        delivery_id=delivery_id,
        status="failed",
        attempts=max_attempts,
        last_error=last_error
    )
    return None


def get_user_id(request: Request) -> str:
    """
    RapidAPI will send X-RapidAPI-User.
    For local testing, we accept X-Demo-User.
    """
    rapidapi_user = request.headers.get("X-RapidAPI-User")
    demo_user = request.headers.get("X-Demo-User")

    user_id = rapidapi_user or demo_user
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Missing user header. Send X-Demo-User (local) or X-RapidAPI-User (RapidAPI)."
        )
    return user_id

def ensure_user(user_id: str):
    if user_id not in USERS:
        USERS[user_id] = {"destinations": [], "events": []}

@app.get("/")
def read_root():
    return {"message": "Webhook API is running"}

@app.post("/v1/destinations")
async def create_destination(request: Request, payload: dict = Body(...)):
    user_id = get_user_id(request)
    ensure_user(user_id)

    url = payload.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="Missing 'url' in request body")

    # Auto-fix missing protocol
    if not (url.startswith("http://") or url.startswith("https://")):
        url = "https://" + url

    destination = {
        "destination_id": f"dst_{uuid.uuid4().hex}",
        "url": url,
        "active": True,
        "created_at": datetime.utcnow().isoformat() + "Z"
    }

    db_destination = {
        "id": destination["destination_id"],
        "user_id": user_id,
        "url": destination["url"],
        "active": destination["active"],
        "created_at": destination["created_at"]
    }
    create_destination_db(db_destination)

    return {
        "status": "created",
        "user_id": user_id,
        "destination": destination
    }

@app.get("/v1/destinations")
async def list_destinations(request: Request):
    user_id = get_user_id(request)
    ensure_user(user_id)
    destinations = list_destinations_db(user_id)
    # keep the response shape similar to before
    return {
        "user_id": user_id,
        "destinations": [
            {
                "destination_id": d["id"],
                "url": d["url"],
                "active": d["active"],
                "created_at": d["created_at"]
            }
            for d in destinations
        ]
    }

@app.get("/v1/deliveries/{delivery_id}")
async def read_delivery_status(request: Request, delivery_id: str):
    user_id = get_user_id(request)
    ensure_user(user_id)

    delivery = get_delivery(delivery_id)
    if not delivery:
        raise HTTPException(status_code=404, detail="Delivery not found")

    if delivery["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    return {"delivery": delivery}

@app.get("/v1/deliveries")
async def list_deliveries(request: Request, limit: int = 20):
    user_id = get_user_id(request)
    ensure_user(user_id)

    deliveries = list_deliveries_for_user(user_id, limit=limit)
    return {"user_id": user_id, "deliveries": deliveries}


@app.post("/v1/ingest/{source}")
async def ingest_webhook(
    source: str,
    request: Request,
    background_tasks: BackgroundTasks,
    payload: dict = Body(...)
):

    user_id = get_user_id(request)
    ensure_user(user_id)

    destination_url = get_active_destination_url(user_id)

    if not destination_url:
        raise HTTPException(
            status_code=400,
            detail="No active destination configured for this user. Create one using POST /v1/destinations"
        )


    event = {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "source": source,
        "event_type": payload.get("event", "unknown"),
        "occurred_at": datetime.utcnow().isoformat() + "Z",
        "data": payload,
        "raw": payload
    }

    # Save event (per user, in-memory)
    USERS[user_id]["events"].append(event)

    # Create delivery record (SQLite)
    delivery_id = f"dly_{uuid.uuid4().hex}"
    delivery = {
        "id": delivery_id,
        "user_id": user_id,
        "source": source,
        "destination_url": destination_url,
        "event_type": event["event_type"],
        "occurred_at": event["occurred_at"],
        "status": "pending",
        "attempts": 0,
        "last_error": None
    }
    create_delivery(delivery)

    # Forward once (retries coming next)
   # Run delivery + retries in the background (do not block the response)
    background_tasks.add_task(attempt_delivery, delivery_id, destination_url, event)
    destination_status = None




    return {
        "status": "accepted",
        "delivery_id": delivery_id,
        "user_id": user_id,
        "destination_url": destination_url,
        "destination_status": destination_status,
        "event": event
    }

