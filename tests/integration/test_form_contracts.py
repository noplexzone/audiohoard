from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse

import pytest
from httpx import AsyncClient


@dataclass
class ParsedForm:
    method: str = "get"
    action: str = ""
    attrs: dict[str, str] = field(default_factory=dict)
    fields: list[tuple[str, str]] = field(default_factory=list)


class FormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.forms: list[ParsedForm] = []
        self._current: ParsedForm | None = None
        self._select_name: str | None = None
        self._select_value: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = {k: v or "" for k, v in attrs}
        if tag == "form":
            self._current = ParsedForm(
                method=attr.get("method", "get").lower(), action=attr.get("action", ""), attrs=attr
            )
            return
        if self._current is None:
            return
        if tag == "input":
            name = attr.get("name")
            if not name:
                return
            input_type = attr.get("type", "text").lower()
            if input_type in {"submit", "button", "image", "file"}:
                return
            if input_type in {"checkbox", "radio"} and "checked" not in attr:
                return
            value = attr.get("value", "")
            if "required" in attr and not value:
                value = f"test-{name}"
            self._current.fields.append((name, value))
        elif tag == "select":
            self._select_name = attr.get("name")
            self._select_value = None
        elif tag == "option" and self._select_name:
            value = attr.get("value", "")
            if self._select_value is None or "selected" in attr:
                self._select_value = value

    def handle_endtag(self, tag: str) -> None:
        if tag == "form" and self._current is not None:
            self.forms.append(self._current)
            self._current = None
        elif tag == "select" and self._current is not None and self._select_name:
            self._current.fields.append((self._select_name, self._select_value or ""))
            self._select_name = None
            self._select_value = None


def _parse_forms(html: str) -> list[ParsedForm]:
    parser = FormParser()
    parser.feed(html)
    return parser.forms


def _same_template_has_script_for_form(html: str, form: ParsedForm) -> bool:
    form_id = form.attrs.get("id")
    if not form_id:
        return False
    return f'getElementById("{form_id}")' in html and "preventDefault()" in html


def _encoded_fields(fields: list[tuple[str, str]]) -> dict[str, str | list[str]]:
    grouped: defaultdict[str, list[str]] = defaultdict(list)
    for key, value in fields:
        grouped[key].append(value)
    return {key: values[0] if len(values) == 1 else values for key, values in grouped.items()}


async def _native_submit(client: AsyncClient, page_url: str, form: ParsedForm) -> Any:
    action = form.action or page_url
    target = urlparse(urljoin(str(client.base_url), action)).path
    data = _encoded_fields(form.fields)
    if form.method == "post":
        return await client.post(target, data=data, follow_redirects=False)
    return await client.get(target, params=data, follow_redirects=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/search", "/downloads", "/settings/download-sources"])
async def test_authenticated_page_forms_replay_as_native_browser_posts(
    client: AsyncClient, path: str
) -> None:
    client.headers.pop("X-CSRF-Token", None)
    page = await client.get(path)
    assert page.status_code == 200
    forms = _parse_forms(page.text)

    for form in forms:
        if form.action == "/logout":
            continue
        if form.attrs.get("data-custom-submit") == "true":
            assert _same_template_has_script_for_form(page.text, form)
            continue

        response = await _native_submit(client, path, form)
        assert response.status_code not in {401, 403, 405, 422}, (
            path,
            form.method,
            form.action,
            response.status_code,
            response.text[:300],
        )
        assert 200 <= response.status_code < 400
        if 300 <= response.status_code < 400:
            location = response.headers["location"]
            followed = await client.get(location, follow_redirects=True)
            assert followed.status_code == 200


@pytest.mark.asyncio
async def test_post_only_ui_gets_redirect_to_get_page(client: AsyncClient) -> None:
    search = await client.get("/search/ui", follow_redirects=False)
    assert search.status_code == 307
    assert search.headers["location"] == "/search"

    downloads = await client.get("/downloads/create", follow_redirects=False)
    assert downloads.status_code == 307
    assert downloads.headers["location"] == "/downloads"


@pytest.mark.asyncio
async def test_base_template_does_not_install_global_form_interceptor(client: AsyncClient) -> None:
    page = await client.get("/search")
    assert page.status_code == 200
    assert 'document.addEventListener("submit"' not in page.text
    assert "window.location.assign" not in page.text


@pytest.mark.asyncio
async def test_login_form_posts_natively_and_renders_errors(
    unauthenticated_client: AsyncClient,
) -> None:
    await unauthenticated_client.post(
        "/api/auth/setup", json={"username": "owner", "password": "Owner-Password-Secure-42"}
    )
    await unauthenticated_client.post(
        "/api/auth/logout", headers={"X-CSRF-Token": unauthenticated_client.cookies["csrf"]}
    )

    bad = await unauthenticated_client.post(
        "/login", data={"username": "owner", "password": "wrong-password"}, follow_redirects=False
    )
    assert bad.status_code == 200
    assert "Invalid username or password" in bad.text

    good = await unauthenticated_client.post(
        "/login",
        data={"username": "owner", "password": "Owner-Password-Secure-42"},
        follow_redirects=False,
    )
    assert good.status_code == 303
    assert good.headers["location"] == "/"
    assert unauthenticated_client.cookies.get("session")
    assert unauthenticated_client.cookies.get("csrf")
