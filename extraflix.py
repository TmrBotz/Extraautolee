"""
╔══════════════════════════════════════════════════════════════╗
║               EXTRAFLIX SCRAPER — Optimized                  ║
║                                                              ║
║  Flow:                                                       ║
║  1. Posts API → latest posts (title, url)                    ║
║  2. MongoDB batch duplicate check (post_url slug se)         ║
║  3. Scrape API → download links (fileInfo, fileSize, url)    ║
║  4. 2GB filter → return ready-to-leech posts                 ║
╚══════════════════════════════════════════════════════════════╝
"""

import re
import os
import asyncio
import logging
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient

log = logging.getLogger("ExtraFlix")

# ─── Config ──────────────────────────────────────────────────────────────────

POSTS_API_URL   = "https://extra-url.tmrbotz.workers.dev/"
SCRAPE_API_URL  = "https://extraapi.tmrbotz.workers.dev/scrape?url={post_url}"
MONGO_URI       = os.environ.get("MONGO_URI", "")
MONGO_DB        = "extraflix"
MONGO_COLLECTION = "processed_posts"
MAX_SIZE_MB     = 2048   # 2GB filter

# ─── MongoDB ─────────────────────────────────────────────────────────────────

_mongo_col = None

def get_mongo_col():
    global _mongo_col
    if _mongo_col is None:
        client      = AsyncIOMotorClient(MONGO_URI)
        _mongo_col  = client[MONGO_DB][MONGO_COLLECTION]
    return _mongo_col


async def get_processed_slugs(slugs: list) -> set:
    """Ek query mein saare already-processed slugs fetch karo."""
    col  = get_mongo_col()
    docs = await col.find(
        {"slug": {"$in": slugs}},
        {"slug": 1, "_id": 0}
    ).to_list(length=None)
    return {doc["slug"] for doc in docs}


async def mark_processed(slug: str, title: str) -> None:
    col = get_mongo_col()
    await col.update_one(
        {"slug": slug},
        {"$set": {"slug": slug, "title": title}},
        upsert=True,
    )
    log.info(f"✅ MongoDB: marked [{slug}]")


# ─── Slug Extractor ──────────────────────────────────────────────────────────

def extract_slug(post_url: str) -> str:
    """
    URL se unique slug nikalo — MongoDB key ke liye.
    'https://e6.extraflix.mobi/uyir-2026-hindi-malayalam/' → 'uyir-2026-hindi-malayalam'
    """
    return post_url.rstrip("/").split("/")[-1]


# ─── Size Parser ─────────────────────────────────────────────────────────────

def parse_size_mb(size_str: str) -> float:
    """
    '635.76 MB' → 635.76
    '1.57 GB'   → 1607.68
    '11.05 GB'  → 11315.2
    """
    s = (size_str or "").strip().upper()
    try:
        num = float(re.sub(r"[^\d.]", "", s))
        if "GB" in s:
            return num * 1024
        return num   # MB
    except Exception:
        return 0.0


# ─── Quality Parser ──────────────────────────────────────────────────────────

def parse_quality(file_info: str) -> str:
    """
    fileInfo string se quality nikalo.
    'Uyir.2026.1080p.JHS.WEB-DL.DUAL.DDP5.1.H.265-ExtraFlix.Pw.mkv - 2.09 GB'
    → '1080p H.265'

    Priority: resolution + codec dono dikhao.
    """
    res_match   = re.search(r"(2160p|1080p|720p|480p)", file_info, re.IGNORECASE)
    codec_match = re.search(r"(H\.265|H\.264|HEVC|AVC)", file_info, re.IGNORECASE)

    resolution = res_match.group(1).lower() if res_match else "unknown"
    codec      = codec_match.group(1).upper() if codec_match else ""

    if codec:
        return f"{resolution} {codec}"
    return resolution


# ─── Posts API ───────────────────────────────────────────────────────────────

async def fetch_latest_posts(session: aiohttp.ClientSession) -> list:
    """
    ExtraFlix Posts API se latest posts fetch karo.

    Returns: [{"title": "...", "url": "...", "slug": "..."}]
    """
    log.info(f"📡 Posts API: {POSTS_API_URL}")
    try:
        async with session.get(
            POSTS_API_URL,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            if resp.status != 200:
                log.warning(f"Posts API: HTTP {resp.status}")
                return []
            data = await resp.json(content_type=None)
    except Exception as e:
        log.warning(f"Posts API error: {e}")
        return []

    posts_raw = data.get("posts", [])
    if not posts_raw:
        log.warning("Posts API: empty posts list")
        return []

    posts = []
    for item in posts_raw:
        title    = (item.get("title") or "").strip()
        post_url = (item.get("url") or "").strip()
        if not post_url:
            continue
        slug = extract_slug(post_url)
        posts.append({
            "title":    title,
            "post_url": post_url,
            "slug":     slug,
        })

    log.info(f"📋 Posts API se {len(posts)} post(s) mile")
    return posts


# ─── Scrape API → Download Links ─────────────────────────────────────────────

async def fetch_download_links(session: aiohttp.ClientSession, post_url: str) -> list:
    """
    ExtraFlix Scrape API se ek post ke download links fetch karo.
    2GB se bade links automatically filter ho jayenge.

    Returns: [{"quality": "1080p H.265", "size": "2.09 GB", "size_mb": 2140.16, "url": "..."}]
    """
    api_url = SCRAPE_API_URL.format(post_url=post_url)
    log.info(f"  🎬 Scrape API: {api_url[:90]}")

    try:
        async with session.get(
            api_url,
            timeout=aiohttp.ClientTimeout(total=40),
        ) as resp:
            if resp.status != 200:
                log.warning(f"  Scrape API: HTTP {resp.status}")
                return []
            data = await resp.json(content_type=None)
    except Exception as e:
        log.warning(f"  Scrape API error: {e}")
        return []

    if not data.get("success"):
        log.warning(f"  Scrape API: success=false — {data.get('error', 'unknown')}")
        return []

    raw_links = data.get("links", [])
    if not raw_links:
        log.warning("  Scrape API: links list empty")
        return []

    links = []
    for item in raw_links:
        file_info = (item.get("fileInfo") or "").strip()
        size_str  = (item.get("fileSize") or "?").strip()
        url       = (item.get("downloadUrl") or "").strip()

        if not url:
            continue

        size_mb = parse_size_mb(size_str)

        # 2GB filter
        if size_mb > MAX_SIZE_MB:
            log.info(f"  ⛔ Skip {size_str} — over 2GB | {file_info[:50]}")
            continue

        quality = parse_quality(file_info)

        links.append({
            "quality":  quality,
            "size":     size_str,
            "size_mb":  size_mb,
            "url":      url,
        })
        log.info(f"  ✅ {quality} ({size_str})")

    return links


# ─── Main Entry ──────────────────────────────────────────────────────────────

async def fetch_new_posts() -> list:
    """
    Pura pipeline run karo — sirf naye posts return karta hai.

    Returns list of:
    {
        "slug":           "uyir-2026-hindi-malayalam",
        "title":          "Uyir (2026) [Hindi-Malayalam] ...",
        "post_url":       "https://e6.extraflix.mobi/uyir-2026-hindi-malayalam/",
        "download_links": [
            {"quality": "1080p H.265", "size": "2.09 GB", "size_mb": 2140.16, "url": "..."},
            ...
        ]
    }
    """
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:

        # Step 1: Latest posts fetch karo
        posts = await fetch_latest_posts(session)
        if not posts:
            log.warning("Posts API se koi post nahi mila")
            return []

        # Step 2: MongoDB batch duplicate check (slug se)
        all_slugs      = [p["slug"] for p in posts]
        processed_slugs = await get_processed_slugs(all_slugs)
        new_count      = len(all_slugs) - len(processed_slugs)
        log.info(f"📊 Already processed: {len(processed_slugs)} | Naye: {new_count}")

        new_posts = []

        for post in posts:
            slug     = post["slug"]
            title    = post["title"]
            post_url = post["post_url"]

            if slug in processed_slugs:
                log.info(f"  ⏭️ Skip (already done): {slug}")
                continue

            log.info(f"🆕 [{slug}]: {title[:60]}")

            # Step 3: Scrape API → download links
            download_links = await fetch_download_links(session, post_url)

            if not download_links:
                log.warning(f"  ❌ No links under 2GB — marking processed")
                await mark_processed(slug, title)
                continue

            new_posts.append({
                "slug":           slug,
                "title":          title,
                "post_url":       post_url,
                "download_links": download_links,
            })

            # Posts ke beech thoda gap
            await asyncio.sleep(0.5)

        log.info(f"✅ Leech ke liye ready: {len(new_posts)} post(s)")
        return new_posts
