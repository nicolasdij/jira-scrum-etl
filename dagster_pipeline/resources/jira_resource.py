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

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def get_sprints_for_board(self, board_id: int) -> list[dict]:
        """Return all sprints (all states) for a given board ID."""
        url = f"{self.base_url}/rest/agile/1.0/board/{board_id}/sprint"
        session = self._session()
        return list(self._paginate(session, url, list_key="values"))

    def get_issues_for_sprint(self, sprint_id: int, project_key: str | None = None) -> list[dict]:
        """Return all issues for a given sprint with the fields required by the pipeline.

        project_key filters results to a single Jira project, preventing issues from
        unrelated projects that share the same sprint from leaking into the pipeline.
        """
        url = f"{self.base_url}/rest/agile/1.0/sprint/{sprint_id}/issue"
        fields = ",".join([
            "summary",
            "status",
            "labels",
            "assignee",
            "customfield_10105",   # story points
            "customfield_10103",   # sprint descriptor array
            "customfield_10834",   # t-shirt size
        ])
        extra_params: dict = {"fields": fields}
        if project_key:
            extra_params["jql"] = f"project = {project_key}"
        session = self._session()
        return list(
            self._paginate(session, url, list_key="issues", extra_params=extra_params)
        )
