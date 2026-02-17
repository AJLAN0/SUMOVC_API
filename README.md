# Rekaz-Hatif Middleware

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Fill in your secrets in .env
```

## Run

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/webhooks/rekaz` | Rekaz webhook receiver |
| POST | `/webhooks/hatif/whatsapp` | Hatif WhatsApp status webhook |

## Testing

### 1. Health check

```bash
curl http://127.0.0.1:8000/health
```

### 2. Rekaz webhook (ReservationCreatedEvent)

```bash
curl -X POST http://127.0.0.1:8000/webhooks/rekaz \
  -H "Authorization: Basic YOUR_REKAZ_BASIC_AUTH" \
  -H "__tenant: YOUR_REKAZ_TENANT_ID" \
  -H "Content-Type: application/json" \
  -d '{
    "Id": "evt-001",
    "EventName": "ReservationCreatedEvent",
    "Data": {
      "customer": {
        "MobileNumber": "966548919392",
        "name": "Test User"
      },
      "number": "R-1001",
      "productName": "Suite",
      "startDate": "2025-06-01"
    }
  }'
```

### 3. Hatif WhatsApp webhook (no signature if HATIF_WEBHOOK_SECRET is empty)

```bash
curl -X POST http://127.0.0.1:8000/webhooks/hatif/whatsapp \
  -H "Content-Type: application/json" \
  -d '{
    "conversationEventId": "abc-123",
    "contactId": "c1",
    "channelId": "ch1",
    "messageId": "m1",
    "direction": "Outbound",
    "status": "Delivered",
    "creationTime": "2025-01-01T10:00:00Z"
  }'
```

### Notes

- `HATIF_WEBHOOK_SECRET`: Configured by Hatif team on your channel. If not set, leave empty and signature verification is skipped.
- `HATIF_SEND_MODE`: Set to `template` (default) to send WhatsApp templates, or `text` to send plain text messages (useful for testing).
