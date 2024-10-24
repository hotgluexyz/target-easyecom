import json
from datetime import datetime, timezone
import requests


class EasyecomAuthenticator:
    """API Authenticator for OAuth 2.0 flows."""
    
    def __init__(self, sink, config_file, auth_endpoint) -> None:
        self.tap_name = sink.name
        self._config = dict(sink.config)
        self._auth_headers = {}
        self._auth_params = {}
        self.logger = sink.logger
        self._auth_endpoint = auth_endpoint
        self._config_file = config_file
        self._target = sink._target
        self.expires_in = self._target.config.get("expires_in", 0)

    @property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        result = {}
        result["Authorization"] = f"Bearer {self._target._config.get('access_token')}"
        return result

    @property
    def auth_endpoint(self) -> str:
        """Get the authorization endpoint.

        Returns:
            The API authorization endpoint if it is set.

        Raises:
            ValueError: If the endpoint is not set.
        """
        if not self._auth_endpoint:
            raise ValueError("Authorization endpoint not set.")
        return self._auth_endpoint

    @property
    def request_body(self) -> dict:
        """Define the OAuth request body for the API."""
        return {
            "email": self.config.get("email"),
            "password": self.config.get("password"),
            "location_key": self.config.get("location_key"),
        }

    def is_token_valid(self) -> bool:
        now = round(datetime.now(timezone.utc).timestamp())
        created_at = self._target._config.get("created_at", 0)

        return now < (created_at + self.expires_in - 60)

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        auth_request_payload = self.request_body
        token_response = requests.post(self.auth_endpoint, data=auth_request_payload)
        try:
            token_last_refreshed = round(datetime.utcnow().timestamp())
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
            token_json = token_response.json()
            token = token_json["data"]["token"]
        except Exception as ex:
            raise RuntimeError(
                f"Failed login, response was '{token_response.json()}'. {ex}"
            )
        self.access_token = token["jwt_token"]
        self.expires_in = token["expires_in"]

        self._target._config["created_at"] = token_last_refreshed
        self._target._config["access_token"] = self.access_token
        self._target._config["expires_in"] = self.expires_in
        with open(self._target.config_file, "w") as outfile:
            json.dump(self._target._config, outfile, indent=4)
