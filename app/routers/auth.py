from __future__ import annotations

import asyncio
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import Response as StarletteResponse

from app.auth import (
    check_login_allowed,
    clear_login_failures,
    create_session,
    get_current_user,
    hash_password,
    login_key,
    record_login_failure,
    require_admin,
    require_mutation,
    setup_complete,
    validate_password,
    verify_password,
)
from app.config import Settings, get_settings
from app.database import get_db
from app.models.auth import AppUser, AuthSession, UserRole
from app.schemas.settings import SettingsSaveRequest
from app.settings_service import save_settings

router = APIRouter()
_setup_owner_lock = asyncio.Lock()


def _safe_next_target(value: object) -> str:
    target = str(value or "").strip()
    if not target.startswith("/") or target.startswith("//") or "\\" in target:
        return "/"
    return target


class Credentials(BaseModel):
    username: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    password: str = Field(min_length=1, max_length=256)


class SetupPayload(Credentials):
    provider_settings: SettingsSaveRequest | None = None


class UserCreate(Credentials):
    role: Literal["admin", "member", "viewer"] = "member"


def _validate_setup_provider_pairs(provider_settings: SettingsSaveRequest | None) -> None:
    if provider_settings is None:
        return
    values = provider_settings.model_dump(exclude_unset=True)
    for provider, url_key, secret_key in (
        ("slskd", "slskd_url", "slskd_api_key"),
        ("prowlarr", "prowlarr_url", "prowlarr_api_key"),
        ("sabnzbd", "sabnzbd_url", "sabnzbd_api_key"),
    ):
        url = values.get(url_key)
        secret = values.get(secret_key)
        if bool(url) != bool(secret):
            raise HTTPException(
                status_code=422,
                detail={"validation_errors": {provider: "URL and API key are required together"}},
            )


def _set_auth_cookies(
    response: Response, token: str, session: AuthSession, settings: Settings
) -> None:
    max_age = settings.session_ttl_seconds
    response.set_cookie(
        "session",
        token,
        httponly=True,
        samesite="strict",
        secure=settings.auth_cookie_secure,
        max_age=max_age,
    )
    response.set_cookie(
        "csrf",
        session.csrf_token,
        httponly=False,
        samesite="strict",
        secure=settings.auth_cookie_secure,
        max_age=max_age,
    )


async def _authenticate_login(
    credentials: Credentials, request: Request, db: AsyncSession, settings: Settings
) -> tuple[str, AuthSession, AppUser]:
    key = login_key(request, credentials.username)
    check_login_allowed(key)
    user = await db.scalar(select(AppUser).where(AppUser.username == credentials.username))
    if user is None or not verify_password(user.password_hash, credentials.password):
        record_login_failure(key)
        raise HTTPException(status_code=401, detail="Invalid username or password")
    clear_login_failures(key)
    token, session = await create_session(db, user, settings)
    return token, session, user


@router.get("/setup", response_class=HTMLResponse, include_in_schema=False)
async def setup_page(
    request: Request, db: Annotated[AsyncSession, Depends(get_db)]
) -> StarletteResponse:
    if await setup_complete(db):
        return RedirectResponse("/login", status_code=307)
    templates: Jinja2Templates = request.app.state.templates
    return templates.TemplateResponse(request, "setup.html", {})


@router.get("/login", response_class=HTMLResponse, include_in_schema=False)
async def login_page(
    request: Request, db: Annotated[AsyncSession, Depends(get_db)]
) -> StarletteResponse:
    if not await setup_complete(db):
        return RedirectResponse("/setup", status_code=307)
    templates: Jinja2Templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "login.html", {"next_target": _safe_next_target(request.query_params.get("next"))}
    )


@router.post("/api/auth/setup", status_code=201)
async def setup_owner(
    payload: SetupPayload,
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> dict[str, str]:
    if await setup_complete(db):
        raise HTTPException(status_code=409, detail="Setup is already complete")
    validate_password(payload.password)
    _validate_setup_provider_pairs(payload.provider_settings)
    password_hash = hash_password(payload.password)
    async with _setup_owner_lock:
        # Refresh the preflight transaction after waiting for another local claimant.
        # The database partial unique index remains the cross-process authority.
        await db.rollback()
        if await setup_complete(db):
            raise HTTPException(status_code=409, detail="Setup is already complete")
        user = AppUser(
            username=payload.username,
            password_hash=password_hash,
            role=UserRole.owner,
        )
        db.add(user)
        try:
            await db.flush()
            token, session = await create_session(db, user, settings)
            if payload.provider_settings is not None:
                await save_settings(db, payload.provider_settings.model_dump(), settings)
            await db.commit()
        except IntegrityError as exc:
            await db.rollback()
            raise HTTPException(status_code=409, detail="Setup is already complete") from exc
    _set_auth_cookies(response, token, session, settings)
    return {"username": user.username, "role": user.role.value, "csrf_token": session.csrf_token}


@router.post("/login", response_class=HTMLResponse, include_in_schema=False)
async def login_form(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> StarletteResponse:
    if not await setup_complete(db):
        return RedirectResponse("/setup", status_code=307)
    form = await request.form()
    next_target = _safe_next_target(form.get("next"))
    templates: Jinja2Templates = request.app.state.templates
    credentials = Credentials(
        username=str(form.get("username", "")), password=str(form.get("password", ""))
    )
    try:
        token, session, _user = await _authenticate_login(credentials, request, db, settings)
    except HTTPException as exc:
        status_code = exc.status_code if exc.status_code == 429 else 200
        detail = exc.detail if isinstance(exc.detail, str) else "Login failed"
        return templates.TemplateResponse(
            request,
            "login.html",
            {"error": detail, "next_target": next_target},
            status_code=status_code,
        )
    response = RedirectResponse(next_target, status_code=303)
    _set_auth_cookies(response, token, session, settings)
    return response


@router.post("/api/auth/login")
async def login(
    payload: Credentials,
    request: Request,
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> dict[str, str]:
    token, session, user = await _authenticate_login(payload, request, db, settings)
    _set_auth_cookies(response, token, session, settings)
    return {"username": user.username, "role": user.role.value, "csrf_token": session.csrf_token}


@router.post("/logout", include_in_schema=False)
async def logout_ui(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    user: Annotated[AppUser, Depends(require_mutation)],
) -> RedirectResponse:
    del user
    session: AuthSession = request.state.auth_session
    await db.delete(session)
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie("session")
    response.delete_cookie("csrf")
    return response


@router.post("/api/auth/logout", status_code=204)
async def logout(
    request: Request,
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
    user: Annotated[AppUser, Depends(require_mutation)],
) -> None:
    del user
    session: AuthSession = request.state.auth_session
    await db.delete(session)
    response.delete_cookie("session")
    response.delete_cookie("csrf")


@router.get("/api/auth/me")
async def me(user: Annotated[AppUser, Depends(get_current_user)]) -> dict[str, str]:
    return {"username": user.username, "role": user.role.value}


@router.post("/api/auth/users", status_code=201)
async def create_user(
    payload: UserCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
    admin: Annotated[AppUser, Depends(require_admin)],
) -> dict[str, str]:
    del admin
    validate_password(payload.password)
    if await db.scalar(select(AppUser.id).where(AppUser.username == payload.username)):
        raise HTTPException(status_code=409, detail="Username already exists")
    user = AppUser(
        username=payload.username,
        password_hash=hash_password(payload.password),
        role=UserRole(payload.role),
    )
    db.add(user)
    await db.flush()
    return {"username": user.username, "role": user.role.value}
