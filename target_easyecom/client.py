"""Easyecom target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from singer_sdk.sinks import RecordSink

from target_hotglue.client import HotglueSink


from target_easyecom.auth import EasyecomAuthenticator


class EasyecomSink(HotglueSink, RecordSink):
    """Easyecom target sink class."""

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params=None, request_data=None, headers=None, verify=True
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers = self.http_headers
        headers.update(self.authenticator.auth_headers)

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
            verify=verify
        )
        self.validate_response(response)
        return response
    
    def validate_response(self, response: requests.Response) -> None:
        try:
            # easyecom API returns the status code in the response body
            # use try/except to avoid errors if the response is not a json
            response.status_code = response.json()['code']
        except:
            # just log the status code and text here
            # the error will be handled by the super class validate_response
            self.logger.error(
                "Could not parse response as JSON. "
                f"Status code: {response.status_code}. "
                f"Response body: {response.text}"
            )

        return super().validate_response(response)

    @property
    def base_url(self) -> str:
        return "https://api.easyecom.io"

    @property
    def authenticator(self):
        return EasyecomAuthenticator(
            self, self._target.config, f"{self.base_url}/access/token"
        )