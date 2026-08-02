from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy import select

from app.database import get_session_factory
from app.models.source_candidate_block import SourceCandidateBlock


async def test_blocklist_page_lists_and_unblocks_source_candidates(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        block = SourceCandidateBlock(
            provider="slskd",
            peer="StarCaller",
            filename="music\\done\\country\\44 - Wrong Track.mp3",
            reason="denied",
        )
        db.add(block)
        await db.commit()
        block_id = block.id

    page = await client.get("/blocklist")

    assert page.status_code == 200
    assert "Source blocklist" in page.text
    assert "StarCaller" in page.text
    assert "44 - Wrong Track.mp3" in page.text
    assert f'action="/blocklist/{block_id}/remove"' in page.text

    removed = await client.post(f"/blocklist/{block_id}/remove", follow_redirects=False)

    assert removed.status_code == 303
    assert removed.headers["location"] == "/blocklist?removed=1"
    async with factory() as db:
        rows = (await db.scalars(select(SourceCandidateBlock))).all()
        assert rows == []


async def test_blocklist_page_empty_state(client: AsyncClient) -> None:
    page = await client.get("/blocklist")

    assert page.status_code == 200
    assert "No blocked sources" in page.text
