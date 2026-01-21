from fastapi import FastAPI, Body, HTTPException, Request
from datetime import datetime
import uuid
import httpx

app = FastAPI()

# TEMP storage (later we replace this with a database)
# Structure:
# USERS[user_id] = { "destinations": [...], "events": [...] }
USERS = {}

def get_user_id(request: Request) -> str:
    """
    RapidAPI will send X-RapidAPI-User.
    For local testing in Swagger, we'll also accept X-Demo-User.
    """
    rapidapi_user = request.headers.get("X-RapidAPI-User")
    demo_user = request.headers.get("X-Demo-User")

    user_id = rapidapi_user or demo_user
    if not user_id:
        # Friendly error for local testing
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

    USERS[user_id]["destinations"].append(destination)

    return {
        "status": "created",
        "user_id": user_id,
        "destination": destination
    }

@app.get("/v1/destinations")
async def list_destinations(request: Request):
    user_id = get_user_id(request)
    ensure_user(user_id)
    return {"user_id": user_id, "destinations": USERS[user_id]["destinations"]}

@app.post("/v1/ingest/{source}")
async def ingest_webhook(source: str, request: Request, payload: dict = Body(...)):
    user_id = get_user_id(request)
    ensure_user(user_id)

    destinations = USERS[user_id]["destinations"]
    active_destinations = [d for d in destinations if d["active"]]

    if len(active_destinations) == 0:
        raise HTTPException(
            status_code=400,
            detail="No active destination configured for this user. Create one using POST /v1/destinations"
        )

    destination_url = active_destinations[-1]["url"]

    event = {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "source": source,
        "event_type": payload.get("event", "unknown"),
        "occurred_at": datetime.utcnow().isoformat() + "Z",
        "data": payload,
        "raw": payload
    }

    # Save event (per user)
    USERS[user_id]["events"].append(event)

    async with httpx.AsyncClient() as client:
        response = await client.post(destination_url, json=event)

    return {
        "status": "forwarded",
        "user_id": user_id,
        "destination_url": destination_url,
        "destination_status": response.status_code,
        "event": event
    }
