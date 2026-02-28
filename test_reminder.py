"""
Quick test: insert a ScheduledMessage with run_at = NOW + 2 min so the
reminder worker picks it up after 2 minutes.

Usage:
    python3 test_reminder.py 966XXXXXXXXX

If no phone is provided, it defaults to the first ADMIN_TO_NUMBERS phone.
The app must be running (uvicorn) so the reminder_worker_loop is active.
"""
import json
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from app.config import settings
from app.database import SessionLocal, init_db
from app.models import ScheduledMessage
from app.services.rekaz import build_template_parameters

init_db()


def main():
    phone = sys.argv[1] if len(sys.argv) > 1 else None

    if not phone:
        admin_phones = settings.admin_numbers()
        if admin_phones:
            phone = admin_phones[0]
        else:
            print("ERROR: pass a phone number as argument or set ADMIN_TO_NUMBERS in .env")
            sys.exit(1)

    db = SessionLocal()
    try:
        # Clean up old test jobs so unique constraint doesn't block us
        old_jobs = db.query(ScheduledMessage).filter(
            ScheduledMessage.reservation_number.like("TEST-%")
        ).all()
        if old_jobs:
            for oj in old_jobs:
                db.delete(oj)
            db.commit()
            print(f"Cleaned up {len(old_jobs)} old test job(s).")

        template_name = "reservation_reminder"
        params = ["Test User"]

        now = datetime.now(timezone.utc)
        run_at = now + timedelta(minutes=2)
        tag = str(int(now.timestamp()))

        job = ScheduledMessage(
            external_event_id=f"test-reminder-{tag}",
            reservation_number=f"TEST-{tag}",
            to_phone=phone,
            template_name=template_name,
            params_json=json.dumps(params, ensure_ascii=False),
            run_at=run_at.replace(tzinfo=None),
            status="pending",
        )

        db.add(job)
        db.commit()
        print(f"OK  Inserted reminder job:")
        print(f"    id:          {job.id}")
        print(f"    to_phone:    {phone}")
        print(f"    template:    {template_name}")
        print(f"    send_mode:   {settings.HATIF_SEND_MODE}")
        print(f"    params:      {params}")
        print(f"    run_at:      {run_at.isoformat()} (NOW + 2 min)")
        print(f"    status:      pending")
        print()
        print("The reminder worker will pick this up in ~2 minutes.")
        print("Watch the app logs for: reminder_sending / reminder_sent_ok / reminder_send_failed")
    except Exception as e:
        db.rollback()
        print(f"ERROR: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
