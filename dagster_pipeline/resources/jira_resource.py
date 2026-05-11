"""Dagster resource for the Jira Agile REST API (Atlassian Cloud).

Authentication: HTTP Basic with email + API token.
"""

from __future__ import annotations

import time
from typing import Any, Generator

import requests
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field


class JiraResource(ConfigurableResource):
    """Thin wrapper around the Jira Agile REST API.

    Configure via environment variables (see .env.example):
        JIRA_BASE_URL   e.g. https://your-org.atlassian.net
        JIRA_EMAIL      your Atlassian account email
        JIRA_API_TOKEN  API token from https://id.atlassian.com/manage-profile/security/api-tokens
    """

    base_url: str = Field(description="Atlassian Cloud base URL, e.g. https://org.atlassian.net")
    email: str = Field(description="Atlassian account email")
    # Plain str — Dagster's ConfigurableResource does not support pydantic.SecretStr.
    # The value is sourced from an env var and never logged.
    api_token: str = Field(description="Jira API token")
    max_results_per_page: int = Field(default=100, description="Page size for paginated requests")
    retry_attempts: int = Field(default=3, description="Number of retries on 429/5xx responses")
    retry_backoff_seconds: float = Field(default=2.0, description="Base backoff between retries")

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _session(self) -> requests.Session:
        session = requests.Session()
        session.auth = (self.email, self.api_token)
        session.headers.update({"Accept": "application/json"})
        return session

    def _get(self, session: requests.Session, url: str, params: dict | None = None) -> Any:
        log = get_dagster_logger()
        params = params or {}
        for attempt in range(1, self.retry_attempts + 1):
            resp = session.get(url, params=params)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = self.retry_backoff_seconds * attempt
                log.warning(f"HTTP {resp.status_code} on {url}; retrying in {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            if not resp.ok:
                log.warning(f"HTTP {resp.status_code} on {url}: {resp.text}")
                resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()  # final raise if all retries exhausted

    def _post(self, session: requests.Session, url: str, body: dict) -> Any:
        log = get_dagster_logger()
        for attempt in range(1, self.retry_attempts + 1):
            resp = session.post(url, json=body)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = self.retry_backoff_seconds * attempt
                log.warning(f"HTTP {resp.status_code} on {url}; retrying in {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            if not resp.ok:
                log.warning(f"HTTP {resp.status_code} on {url}: {resp.text}")
                resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()  # final raise if all retries exhausted

    def _paginate(
        self,
        session: requests.Session,
        url: str,
        list_key: str,
        extra_params: dict | None = None,
    ) -> Generator[dict, None, None]:
        """Yield items from a paginated Jira endpoint (startAt / maxResults / total)."""
        params = {**(extra_params or {}), "maxResults": self.max_results_per_page, "startAt": 0}
        while True:
            data = self._get(session, url, params)
            items = data.get(list_key, [])
            yield from items
            params["startAt"] += len(items)
            if params["startAt"] >= data.get("total", params["startAt"]):
                break
            if not items:
                break

    def _paginate_post(
        self,
        session: requests.Session,
        url: str,
        body: dict,
        list_key: str,
    ) -> Generator[dict, None, None]:
        """Yield items from a cursor-paginated Jira v3 endpoint (nextPageToken)."""
        page_body = {**body, "maxResults": self.max_results_per_page}
        while True:
            data = self._post(session, url, page_body)
            items = data.get(list_key, [])
            yield from items
            next_token = data.get("nextPageToken")
            if not next_token or not items:
                break
            page_body = {**page_body, "nextPageToken": next_token}

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def get_sprints_for_board(self, board_id: int) -> list[dict]:
        """Return all sprints (all states) for a given board ID."""
        url = f"{self.base_url}/rest/agile/1.0/board/{board_id}/sprint"
        session = self._session()
        return list(self._paginate(session, url, list_key="values"))

    def get_issues_for_sprint(self, sprint_id: int) -> list[dict]:
        """Return all issues for a given sprint with the fields required by the pipeline.

        Uses the REST API v3 /search/jql endpoint (POST, cursor-based pagination) so the
        board's own saved filter is bypassed and issues from all projects are included.
        """
        url = f"{self.base_url}/rest/api/3/search/jql"
        fields = [
            "summary",
            "status",
            "labels",
            "assignee",
            "customfield_10105",   # story points
            "customfield_10103",   # sprint descriptor array
            "customfield_10834",   # t-shirt size
        ]
        session = self._session()
        return list(
            self._paginate_post(
                session,
                url,
                body={"jql": f"sprint = {sprint_id} AND issuetype in (Story, Task, Bug)", "fields": fields},
                list_key="issues",
            )
        )
