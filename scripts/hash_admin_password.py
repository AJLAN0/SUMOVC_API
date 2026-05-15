#!/usr/bin/env python3
"""Print a bcrypt hash for ADMIN_PASSWORD_HASH (Railway / .env)."""
import getpass
import sys

import bcrypt


def main() -> None:
    if len(sys.argv) > 1:
        password = sys.argv[1]
    else:
        password = getpass.getpass("Admin password: ")
        confirm = getpass.getpass("Confirm: ")
        if password != confirm:
            print("Passwords do not match.", file=sys.stderr)
            sys.exit(1)
    hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
    print(hashed.decode("utf-8"))


if __name__ == "__main__":
    main()
