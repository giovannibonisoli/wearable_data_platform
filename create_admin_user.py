#!/usr/bin/env python3
"""
Script to create an admin user.

Usage:
    python create_admin_user.py <username> <password> <full_name>
"""

import sys
import bcrypt
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from database.connection import ConnectionManager
from database.orm_models import UserModel, UserRole


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <username> <password> <full_name>")
        sys.exit(1)

    username = sys.argv[1]
    password = sys.argv[2]
    full_name = sys.argv[3]

    password_hash = bcrypt.hashpw(
        password.encode('utf-8'), bcrypt.gensalt()
    ).decode('utf-8')

    with ConnectionManager() as db:
        existing = db.session.query(UserModel).filter(UserModel.username == username).first()
        if existing:
            print(f"Error: User '{username}' already exists")
            sys.exit(1)

        user = UserModel(
            username=username,
            password_hash=password_hash,
            full_name=full_name,
            role=UserRole.ADMIN.value,
            created_at=datetime.now(),
        )
        db.session.add(user)
        db.session.commit()

        print(f"Admin user created: {username} (ID: {user.id})")


if __name__ == "__main__":
    main()