from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

from database import ConnectionManager, DeviceRepository, AuthorizationRepository, Device
from services.integrations.fitbit import (
    FitbitClient,
    generate_state,
    get_tokens,
    generate_code_verifier,
    generate_code_challenge,
    generate_auth_url,
)
from services.integrations.emails import send_email
from services.result_enums import AddDeviceResult, SendAuthEmailResult, AuthGrantResult

import base64
import json


class DeviceService:
    """
    Service for device authorization and basic device info management.

    Encapsulates business logic for handling device authorization flows
    and retrieving device metadata from the Fitbit API.
    """

    def __init__(self, connection_manager: ConnectionManager):
        """
        Args:
            connection_manager: Active ConnectionManager instance.
        """
        self.conn = connection_manager
        self.auth_repo = AuthorizationRepository(connection_manager)
        self.device_repo = DeviceRepository(connection_manager)

    def get_devices_info_by_admin_user(self, admin_user_id: int) -> list[dict]:
        devices = self.device_repo.get_by_admin_user(admin_user_id)

        devices_data = []
        for device in devices:
            devices_data.append({
                "id": device.id,
                "email_address": device.email_address,
                "device_type": device.device_type if device.device_type else "",
                "auth_status": device.authorization_status,
                "is_pending_auth": self.auth_repo.check_exists(device.id),
            })

        return devices_data

    def add_new_device(self, admin_user_id: int, email_address: str) -> AddDeviceResult:
        existing = self.device_repo.get_by_email(email_address)

        if existing:
            return AddDeviceResult.ALREADY_EXISTS

        device_id = self.device_repo.create(
            admin_user_id=admin_user_id,
            email_address=email_address,
        )

        return AddDeviceResult.ADDED if device_id else AddDeviceResult.ERROR

    def update_devices_info_by_admin_user(self, admin_user_id: int) -> List[str]:
        devices = self.device_repo.get_all_authorized_by_admin_user(admin_user_id)

        errors = []
        for device in devices:
            try:
                access_token, refresh_token = self.device_repo.get_tokens(device.id)

                # One client per device: auto-refreshes and persists tokens on 401
                client = FitbitClient(
                    access_token=access_token,
                    refresh_token=refresh_token,
                    on_tokens_updated=lambda a, r: self.device_repo.update_tokens(device.id, a, r),
                )

                device_data = client.get_device_info()

                device_result = self.device_repo.update_device_type(device.id, device_data["deviceVersion"])
                last_sync_result = self.device_repo.update_last_synch(device.id, device_data["lastSyncTime"])

                if not device_result or not last_sync_result:
                    errors.append(device.email_address)

            except Exception as e:
                print(e)
                errors.append(device.email_address)

        return errors

    def send_authorization_email(self, device_id: int) -> tuple[str, SendAuthEmailResult]:
        device = self.device_repo.get_by_id(device_id)
        email_address = device.email_address

        code_verifier = generate_code_verifier()

        state_data = {
            "email_address": email_address,
            "random": generate_state(),
        }
        state = base64.urlsafe_b64encode(json.dumps(state_data).encode()).decode()

        code_challenge = generate_code_challenge(code_verifier)
        auth_url = generate_auth_url(code_challenge, state)

        email_subject = "Autorizzazione Fitbit - Lively Ageing"

        email_html = f"""
            <html>
            <body>
                <h2>Autorizzazione Fitbit</h2>
                <p>Ciao,</p>
                <p>Per autorizzare l'accesso ai tuoi dati Fitbit, clicca sul link qui sotto:</p>
                <p><a href="{auth_url}">Autorizza Fitbit</a></p>
                <p>Oppure copia e incolla questo link nel tuo browser:</p>
                <p>{auth_url}</p>
                <br>
                <p>Grazie,<br>Team Lively Ageing</p>
            </body>
            </html>
            """

        email_text = f"""
            Autorizzazione Fitbit

            Ciao,

            Per autorizzare l'accesso ai tuoi dati Fitbit, copia e incolla questo link nel tuo browser:

            {auth_url}

            Grazie,
            Team Lively Ageing
            """

        if send_email(email_address, email_subject, email_html, email_text):
            if self.auth_repo.store_pending_auth(device_id, state, code_verifier):
                return email_address, SendAuthEmailResult.SUCCESS
            else:
                return email_address, SendAuthEmailResult.ERROR_STORING_PENDING_AUTH
        else:
            return email_address, SendAuthEmailResult.EMAIL_SENDING_ERROR

    def handle_authorization_grant(self, code: str, state: str) -> AuthGrantResult:
        try:
            state_data = json.loads(base64.urlsafe_b64decode(state.encode()).decode())
            email_address = state_data.get("email_address")
        except Exception:
            return AuthGrantResult.MISSING_AUTH_INFO

        if not email_address:
            return AuthGrantResult.EMAIL_NOT_FOUND

        pending_auth = self.auth_repo.get_by_state(state)
        if not pending_auth:
            return AuthGrantResult.INVALID_AUTH_LINK

        code_verifier = pending_auth["code_verifier"]

        access_token, refresh_token = get_tokens(code, code_verifier)
        if not access_token or not refresh_token:
            return AuthGrantResult.ERROR_RETRIEVE_TOKENS

        device = self.device_repo.get_by_email(email_address)

        results = [
            self.device_repo.update_tokens(device.id, access_token, refresh_token),
            self.device_repo.update_status(device.id, "authorized"),
            self.auth_repo.delete_by_state(state),
        ]

        return AuthGrantResult.SUCCESS if all(results) else AuthGrantResult.ERROR_STATE_UPDATE

    def deactivate_device(self, device_id: int) -> None:
        self.device_repo.update_status(device_id, "non_active")
