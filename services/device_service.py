
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

from database import ConnectionManager, DeviceRepository, AuthorizationRepository, Device 
from auth import generate_state, get_tokens, generate_code_verifier, generate_code_challenge, generate_auth_url, get_device_info
from emails import send_email

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


    def add_new_device(self, admin_user_id: int, email_address: str) -> str:
        
        existing = self.device_repo.get_by_email(email_address)

        if existing:
            return "already_exists"
            
        # Create new device
        device_id = self.device_repo.create(
            admin_user_id=admin_user_id,
            email_address=email_address
        )
            
        if device_id:
            return "added"
        else:
            return "error"

    
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


    def send_authorization_email(self, device_id: int) -> str:

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

                return email_address, "success"
            else:
                return email_address, "error_storing_pending_auth"
        else:
            return email_address, "email_sending_error"