
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

from database import ConnectionManager, DeviceRepository, AuthorizationRepository, Device
from services.integrations.fitbit import generate_state, get_tokens, generate_code_verifier, generate_code_challenge, generate_auth_url, get_device_info
from services.integrations.emails import send_email
from services.result_enums import AddDeviceResult, SendAuthEmailResult, AuthGrantResult

import base64
import json

class DeviceService:
    """
    Service for retrieving.
    
    This service encapsulates business logic for handling device authorization and get basic
    info.
    """
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the service with a connection manager.
        
        Args:
            connection_manager: Active ConnectionManager instance
        """
        self.conn = connection_manager
        self.auth_repo = AuthorizationRepository(connection_manager)
        self.device_repo = DeviceRepository(connection_manager)

    
    def get_devices_info_by_admin_user(self, admin_user_id: int) -> list[dict]:

        devices = self.device_repo.get_by_admin_user(admin_user_id)
        
        devices_data = []
        for device in devices:
            auth_status = device.authorization_status

            devices_data.append({
                    "id": device.id,
                    "email_address": device.email_address,
                    "device_type": device.device_type if device.device_type else "",
                    "auth_status": auth_status,
                    "is_pending_auth": self.auth_repo.check_exists(device.id)
                })

        return devices_data


    def add_new_device(self, admin_user_id: int, email_address: str) -> AddDeviceResult:
        existing = self.device_repo.get_by_email(email_address)

        if existing:
            return AddDeviceResult.ALREADY_EXISTS

        # Create new device
        device_id = self.device_repo.create(
            admin_user_id=admin_user_id,
            email_address=email_address
        )

        if device_id:
            return AddDeviceResult.ADDED
        else:
            return AddDeviceResult.ERROR

    
    def update_devices_info_by_admin_user(self, admin_user_id: int) -> List[str]:

        devices = self.device_repo.get_all_authorized_by_admin_user(admin_user_id)

        errors = []
        for device in devices:
            try:
                access_token, _ = self.device_repo.get_tokens(device.id)
            
                device_data = get_device_info(access_token)

                device_result = self.device_repo.update_device_type(device.id, device_data['deviceVersion'])      
                last_sync_result = self.device_repo.update_last_synch(device.id, device_data['lastSyncTime'])
            
                if not device_result or not last_sync_result:
                    errors.append(device.email_address)
                
            except Exception as e:
                errors.append(device.email_address)

        return errors


    def send_authorization_email(self, device_id: int) -> tuple[str, SendAuthEmailResult]:
        device = self.device_repo.get_by_id(device_id)
        email_address = device.email_address

        # Generate code_verifier and store it temporarily with email as key
        code_verifier = generate_code_verifier()

        # Create state that includes email (encoded for security)
        state_data = {
            'email_address': email_address,
            'random': generate_state()  # mantieni randomness per sicurezza
        }

        # Encode the state data
        state = base64.urlsafe_b64encode(json.dumps(state_data).encode()).decode()

        code_challenge = generate_code_challenge(code_verifier)
        auth_url = generate_auth_url(code_challenge, state)

        email_subject = 'Autorizzazione Fitbit - Lively Ageing'

        # Email content
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

        # Email content in simple text
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
            email_address = state_data.get('email_address')
        except Exception:
            return AuthGrantResult.MISSING_AUTH_INFO

        if not email_address:
            return AuthGrantResult.EMAIL_NOT_FOUND

        # Retrieve code_verifier from database
        pending_auth = self.auth_repo.get_by_state(state)

        if not pending_auth:
            return AuthGrantResult.INVALID_AUTH_LINK

        code_verifier = pending_auth['code_verifier']

        # Get tokens from Fitbit
        access_token, refresh_token = get_tokens(code, code_verifier)
        if not access_token or not refresh_token:
            return AuthGrantResult.ERROR_RETRIEVE_TOKENS

        device = self.device_repo.get_by_email(email_address)

        results = [
            self.device_repo.update_tokens(device.id, access_token, refresh_token),
            self.device_repo.update_status(device.id, 'authorized'),
            self.auth_repo.delete_by_state(state)
        ]

        if all(results):
            return AuthGrantResult.SUCCESS
        else:
            return AuthGrantResult.ERROR_STATE_UPDATE
        

    def deactivate_device(self, device_id: int) -> None:

        self.device_repo.update_status(device_id, 'non_active')