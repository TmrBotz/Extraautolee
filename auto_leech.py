"""
╔══════════════════════════════════════════════════════════════╗
║               AUTO LEECH MODULE                              ║
║                                                              ║
║  Har 10 min mein ExtraFlix API call karta hai.               ║
║  Naye posts ke saare download_links download karke           ║
║  LEECH_GROUP mein DL+UL hota hai.                            ║
║  Upload ke baad media AUTO_LEECH_CHANNEL mein copy hoti hai  ║
║  (without forward tag — copy_message use hota hai).          ║
║  Thumbnail: ytthumb API | Duration: ffprobe                  ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import re
import asyncio
import logging
import time
import aiohttp
import tempfile

from pyrogram import Client

from config import (
    DOWNLOAD_DIR, PROGRESS_UPDATE_INTERVAL, MAX_FILE_SIZE,
    LEECH_GROUP, AUTO_LEECH_CHANNEL
)
from helpers import (
    human_size, format_eta, make_progress_bar,
    detect_media_type, extract_filename
)

# ─── Config ──────────────────────────────────────────────────────────────────

THUMB_API_BASE  = "https://skybcnd-84ys.onrender.com/poster?search="
POLL_INTERVAL   = 10 * 60        # 10 minutes
CHUNK_SIZE      = 1024 * 1024    # 1 MB

log = logging.getLogger("AutoLeech")

# ExtraFlix R2 Cloudflare links ke liye headers
# R2 pre-signed URLs public hain — sirf User-Agent aur Accept chahiye
EXTRAFLIX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Encoding": "identity",
}


# ─── Thumbnail Fetch ─────────────────────────────────────────────────────────

def extract_movie_name(title: str) -> str:
    """
    Post title se clean movie name nikalo thumbnail API ke liye.

    Series: "The Family Man S02E01 Hindi 720p" → "The Family Man S02"
    Movie:  "Dhurandhar (2025) Hindi Movie HD"  → "Dhurandhar 2025"
    """
    # Series detect karo — S01, S02E03, S01E01 pattern
    series_match = re.search(r'\bS(\d{1,2})(?:E\d+)?\b', title, re.IGNORECASE)
    if series_match:
        season_num = int(series_match.group(1))
        name = title[:series_match.start()].strip().rstrip('-').strip()
        return f"{name} S{season_num:02d}"

    # Movie — name + year
    year_match = re.match(r"^(.+?)\s*[\(\[]?(\d{4})[\)\]]?", title)
    if year_match:
        name = year_match.group(1).strip()
        year = year_match.group(2)
        return f"{name} {year}"

    # Fallback
    return " ".join(title.split()[:3])


async def fetch_thumbnail(title: str) -> str | None:
    """
    ytthumb API se thumbnail URL fetch karo.
    Returns: imgbb URL string ya None
    """
    movie_name = extract_movie_name(title)
    encoded    = movie_name.replace(" ", "+")
    url        = f"{THUMB_API_BASE}{encoded}"

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    log.warning(f"Thumbnail API: HTTP {resp.status} for '{movie_name}'")
                    return None
                data = await resp.json(content_type=None)
                if data.get("success"):
                    thumb_url = data.get("thumbnail", {}).get("imgbb")
                    log.info(f"🖼️ Thumbnail mila: {thumb_url}")
                    return thumb_url
                else:
                    log.warning(f"Thumbnail API: success=false for '{movie_name}'")
                    return None
    except Exception as e:
        log.warning(f"Thumbnail fetch error: {e}")
        return None


async def download_thumbnail(thumb_url: str) -> str | None:
    """
    Thumbnail URL se image download karke temp file mein save karo.
    Returns: local file path ya None
    """
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(thumb_url) as resp:
                if resp.status != 200:
                    return None
                tmp = tempfile.NamedTemporaryFile(
                    delete=False, suffix=".jpg", dir=DOWNLOAD_DIR
                )
                tmp.write(await resp.read())
                tmp.close()
                log.info(f"🖼️ Thumbnail saved: {tmp.name}")
                return tmp.name
    except Exception as e:
        log.warning(f"Thumbnail download error: {e}")
        return None


# ─── Video Duration (ffprobe) ────────────────────────────────────────────────

async def get_video_duration(file_path: str) -> int:
    """
    ffprobe se video duration seconds mein nikalo.
    Returns: int seconds (0 if failed)
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        duration_str = stdout.decode().strip()
        if duration_str:
            return int(float(duration_str))
    except Exception as e:
        log.warning(f"ffprobe duration error: {e}")
    return 0


# ─── ExtraFlix Scraper Import ─────────────────────────────────────────────────

from extraflix import fetch_new_posts, mark_processed


# ─── Custom Downloader (ExtraFlix R2 Cloudflare links) ───────────────────────

async def download_extraflix(url: str, progress_cb=None):
    """
    ExtraFlix R2 Cloudflare pre-signed links ke liye downloader.
    Returns: (file_path, filename, content_type, total_bytes)
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    timeout = aiohttp.ClientTimeout(connect=30, sock_read=300, total=None)

    async with aiohttp.ClientSession(
        headers=EXTRAFLIX_HEADERS, timeout=timeout
    ) as session:
        async with session.get(url, allow_redirects=True) as resp:

            if resp.status == 403:
                raise Exception("403 Forbidden — ExtraFlix ne access deny kiya.")
            if resp.status == 404:
                raise Exception("404 Not Found — link exist nahi karta.")
            if resp.status not in (200, 206):
                raise Exception(f"HTTP {resp.status} — download fail.")

            content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()
            content_disp = resp.headers.get("Content-Disposition", "")
            total_size   = int(resp.headers.get("Content-Length", 0))

            if total_size > MAX_FILE_SIZE:
                raise Exception(
                    f"File too large: {human_size(total_size)} > {human_size(MAX_FILE_SIZE)}"
                )

            filename  = extract_filename(str(resp.url), content_disp, content_type)
            # Prefix add karo filename mein
            name, ext = os.path.splitext(filename)
            filename  = f"[@Skyhub4u] {name}{ext}"
            file_path = os.path.join(DOWNLOAD_DIR, filename)

            # Duplicate name handle karo
            base, ext = os.path.splitext(file_path)
            counter = 1
            while os.path.exists(file_path):
                file_path = f"{base}_{counter}{ext}"
                counter += 1

            downloaded   = 0
            last_cb_time = 0.0

            with open(file_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)

                    if downloaded > MAX_FILE_SIZE:
                        f.close()
                        os.remove(file_path)
                        raise Exception("File size 2GB se zyada ho gayi!")

                    now = time.monotonic()
                    if progress_cb and (now - last_cb_time) >= 0:
                        last_cb_time = now
                        try:
                            await progress_cb(downloaded, total_size or downloaded)
                        except Exception:
                            pass

            if progress_cb:
                try:
                    await progress_cb(downloaded, downloaded)
                except Exception:
                    pass

            return file_path, filename, content_type, downloaded


# ─── Copy Media to Auto Leech Channel (Without Forward Tag) ──────────────────

async def copy_to_auto_leech_channel(client: Client, sent_msg, caption: str) -> bool:
    """
    Leech Group mein upload hua message AUTO_LEECH_CHANNEL mein
    copy karo — without forward tag (copy_message use karta hai).

    sent_msg: Pyrogram Message object jo leech group mein upload hua.
    caption: Auto leech channel ke liye caption.
    Returns: True on success, False on failure.
    """
    if not AUTO_LEECH_CHANNEL:
        log.warning("AUTO_LEECH_CHANNEL set nahi hai — copy skip")
        return False

    try:
        await client.copy_message(
            chat_id=AUTO_LEECH_CHANNEL,
            from_chat_id=sent_msg.chat.id,
            message_id=sent_msg.id,
            caption=caption,
        )
        log.info(f"✅ Copied to AUTO_LEECH_CHANNEL {AUTO_LEECH_CHANNEL} (no forward tag)")
        return True
    except Exception as e:
        log.warning(f"❌ copy_message failed: {e}")
        return False


# ─── Single Link Leech ────────────────────────────────────────────────────────

async def leech_one_link(
    client: Client,
    link: dict,
    post: dict,
    link_index: int,
    total_links: int,
    thumb_path: str | None,     # Pre-fetched thumbnail local path
) -> bool:
    url      = link.get("url", "")
    quality  = link.get("quality", "Unknown")
    size     = link.get("size", "?")
    title    = post.get("title", "Unknown Movie")
    post_url = post.get("post_url", "")

    # ── Leech group check ────────────────────────────────────────────────────
    if not LEECH_GROUP:
        log.error("LEECH_GROUP env variable set nahi hai! Leech abort.")
        return False

    log.info(f"  [{link_index}/{total_links}] Leeching: {quality} ({size}) — {title}")

    # Progress messages LEECH_GROUP mein jayenge
    try:
        status_msg = await client.send_message(
            chat_id=LEECH_GROUP,
            text=(
                f"⏳ **Auto Leech Start...**\n\n"
                f"🎬 `{title}`\n"
                f"🎞️ Quality: `{quality}` | 📦 Size: `{size}`\n"
                f"[{link_index}/{total_links}] Processing..."
            ),
        )
    except Exception as e:
        log.warning(f"Leech group status message send failed: {e}")
        status_msg = None

    start_time  = time.monotonic()
    file_path   = None
    last_update = [0.0]

    async def safe_edit(text: str):
        if status_msg:
            try:
                await status_msg.edit_text(text)
            except Exception:
                pass

    try:
        # ── PHASE 1: DOWNLOAD (Leech Group mein progress) ───────────────────
        dl_start = [time.monotonic()]

        async def on_dl_progress(downloaded: int, total: int):
            now = time.monotonic()
            if (now - last_update[0]) < PROGRESS_UPDATE_INTERVAL:
                return
            last_update[0] = now
            elapsed = now - dl_start[0]
            speed   = downloaded / elapsed if elapsed > 0 else 0
            eta     = (total - downloaded) / speed if speed > 0 and total > downloaded else 0
            bar     = make_progress_bar(downloaded, total)
            total_s = human_size(total) if total > 0 else "?"
            await safe_edit(
                f"⬇️ **Downloading...**\n\n"
                f"🎬 `{title}`\n"
                f"🎞️ `{quality}` | 📦 `{size}`\n\n"
                f"{bar}\n\n"
                f"📥 `{human_size(downloaded)}` / `{total_s}`\n"
                f"⚡ Speed: `{human_size(int(speed))}/s`\n"
                f"⏱️ ETA: `{format_eta(eta)}`"
            )

        dl_start[0] = time.monotonic()
        file_path, filename, content_type, file_size = await download_extraflix(
            url, progress_cb=on_dl_progress,
        )
        log.info(f"    ✅ Downloaded: {filename} ({human_size(file_size)})")

        # ── PHASE 2: DURATION (ffprobe) ─────────────────────────────────────
        media_type = detect_media_type(content_type, filename)
        duration   = 0
        if media_type == "video":
            await safe_edit(
                f"🔍 **Duration check...**\n\n"
                f"🎬 `{title}` | 🎞️ `{quality}`"
            )
            duration = await get_video_duration(file_path)
            log.info(f"    ⏱️ Duration: {duration}s")

        # ── PHASE 3: UPLOAD → LEECH GROUP mein ─────────────────────────────
        last_update[0] = 0.0
        ul_start = [time.monotonic()]

        await safe_edit(
            f"⬆️ **Uploading to Leech Group...**\n\n"
            f"🎬 `{title}`\n"
            f"🎞️ `{quality}` | 📦 {human_size(file_size)}"
        )

        async def on_ul_progress(current: int, total: int):
            now = time.monotonic()
            if (now - last_update[0]) < PROGRESS_UPDATE_INTERVAL:
                return
            last_update[0] = now
            elapsed = now - ul_start[0]
            speed   = current / elapsed if elapsed > 0 else 0
            eta     = (total - current) / speed if speed > 0 and total > current else 0
            bar     = make_progress_bar(current, total)
            await safe_edit(
                f"⬆️ **Uploading...**\n\n"
                f"🎬 `{title}`\n"
                f"🎞️ `{quality}` | 📦 {human_size(file_size)}\n\n"
                f"{bar}\n\n"
                f"📤 `{human_size(current)}` / `{human_size(total)}`\n"
                f"⚡ Speed: `{human_size(int(speed))}/s`\n"
                f"⏱️ ETA: `{format_eta(eta)}`"
            )

        ul_start[0] = time.monotonic()

        # Caption — leech group ke liye (internal)
        leech_caption = (
            f"🎬 **{title}**\n\n"
            f"🎞️ Quality : `{quality}`\n"
            f"📦 Size    : `{size}`\n\n"
        )

        # Auto leech channel ke liye caption (same — copy_message override karega)
        auto_leech_caption = leech_caption

        common = dict(
            chat_id=LEECH_GROUP,        # ← Upload LEECH_GROUP mein hoga
            caption=leech_caption,
            progress=on_ul_progress,
        )

        # ── Send to Leech Group ──────────────────────────────────────────────
        sent_msg = None

        if media_type == "video":
            sent_msg = await client.send_video(
                video=file_path,
                duration=duration if duration > 0 else None,
                thumb=thumb_path,
                supports_streaming=True,
                **common,
            )
        elif media_type == "audio":
            sent_msg = await client.send_audio(audio=file_path, **common)
        elif media_type == "gif":
            sent_msg = await client.send_animation(animation=file_path, **common)
        elif media_type == "photo":
            sent_msg = await client.send_photo(
                photo=file_path,
                caption=leech_caption,
                chat_id=LEECH_GROUP,
                progress=on_ul_progress,
            )
        else:
            sent_msg = await client.send_document(
                document=file_path, file_name=filename, **common
            )

        elapsed = time.monotonic() - start_time
        log.info(f"    ✅ Uploaded to Leech Group: {filename} in {elapsed:.1f}s")

        # ── PHASE 4: AUTO LEECH CHANNEL mein copy (without forward tag) ─────
        if sent_msg:
            await safe_edit(
                f"📤 **Auto Leech Channel mein copy ho raha hai...**\n\n"
                f"🎬 `{title}` | 🎞️ `{quality}`"
            )
            await copy_to_auto_leech_channel(client, sent_msg, auto_leech_caption)

        # Status message delete karo leech group se (clean up)
        if status_msg:
            try:
                await status_msg.delete()
            except Exception:
                pass

        return True

    except Exception as e:
        log.warning(f"    ❌ Failed [{quality}]: {e}")
        await safe_edit(
            f"❌ **Failed!**\n\n"
            f"🎬 `{title}`\n"
            f"🎞️ `{quality}`\n\n"
            f"`{str(e)[:200]}`"
        )
        return False

    finally:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass


# ─── Process One Post ────────────────────────────────────────────────────────

async def process_post(client: Client, post: dict) -> bool:
    """
    Ek post ke saare download links leech karo.
    extraflix.py se aaya hua post dict expect karta hai.
    """
    slug           = post["slug"]
    title          = post["title"]
    download_links = post["download_links"]

    log.info(f"🎬 Processing [{slug}]: {title} — {len(download_links)} link(s)")

    # Thumbnail ek baar fetch karo — sab links ke liye reuse
    thumb_url  = await fetch_thumbnail(title)
    thumb_path = None
    if thumb_url:
        thumb_path = await download_thumbnail(thumb_url)

    # Leech Group mein announcement
    if LEECH_GROUP:
        try:
            await client.send_message(
                chat_id=LEECH_GROUP,
                text=(
                    f"🆕 **New Movie Auto-Leech Start!**\n\n"
                    f"🎬 **{title}**\n"
                    f"📥 Total Links: `{len(download_links)}`\n"
                    f"⏳ Processing all qualities..."
                ),
            )
        except Exception as e:
            log.warning(f"Leech group announcement failed: {e}")

    total         = len(download_links)
    success_count = 0
    total_start   = time.monotonic()

    for idx, link in enumerate(download_links, start=1):
        ok = await leech_one_link(client, link, post, idx, total, thumb_path)
        if ok:
            success_count += 1
        if idx < total:
            await asyncio.sleep(3)

    # Thumbnail temp file cleanup
    if thumb_path and os.path.exists(thumb_path):
        try:
            os.remove(thumb_path)
        except Exception:
            pass

    # MongoDB mein mark processed (slug se)
    await mark_processed(slug, title)

    # Final summary — LEECH GROUP mein
    total_elapsed = time.monotonic() - total_start
    links_summary = "\n".join(
        f"  {'✅' if i < success_count else '❌'} `{lnk.get('quality','?')}` — {lnk.get('size','?')}"
        for i, lnk in enumerate(download_links)
    )

    if LEECH_GROUP:
        try:
            await client.send_message(
                chat_id=LEECH_GROUP,
                text=(
                    f"✅ **Upload Done!**\n\n"
                    f"🎬 `{title}`\n\n"
                    f"{links_summary}\n\n"
                    f"⏱️ Total: `{format_eta(total_elapsed)}`\n"
                    f"📢 Media auto leech channel mein copy ho gayi (no forward tag)"
                ),
            )
        except Exception as e:
            log.warning(f"Leech group summary failed: {e}")

    return True


# ─── Process Post (No MongoDB) — /extraflix command ke liye ──────────────────

async def process_post_no_mongo(client: Client, post: dict) -> bool:
    """
    process_post ka same kaam — bas MongoDB mark_processed skip karta hai.
    /extraflix {url} command ke liye use hota hai.
    """
    title          = post["title"]
    download_links = post["download_links"]

    log.info(f"🎬 /extraflix Manual Leech: {title} — {len(download_links)} link(s)")

    # Thumbnail ek baar fetch karo — sab links ke liye reuse
    thumb_url  = await fetch_thumbnail(title)
    thumb_path = None
    if thumb_url:
        thumb_path = await download_thumbnail(thumb_url)

    # Leech Group mein announcement
    if LEECH_GROUP:
        try:
            await client.send_message(
                chat_id=LEECH_GROUP,
                text=(
                    f"🆕 **Manual ExtraFlix Leech Start!**\n\n"
                    f"🎬 **{title}**\n"
                    f"📥 Total Links: `{len(download_links)}`\n"
                    f"⏳ Processing all qualities..."
                ),
            )
        except Exception as e:
            log.warning(f"Leech group announcement failed: {e}")

    total         = len(download_links)
    success_count = 0
    total_start   = time.monotonic()

    for idx, link in enumerate(download_links, start=1):
        ok = await leech_one_link(client, link, post, idx, total, thumb_path)
        if ok:
            success_count += 1
        if idx < total:
            await asyncio.sleep(3)

    # Thumbnail temp file cleanup
    if thumb_path and os.path.exists(thumb_path):
        try:
            os.remove(thumb_path)
        except Exception:
            pass

    # NOTE: mark_processed() intentionally skipped — no duplicate filter

    # Final summary
    total_elapsed = time.monotonic() - total_start
    links_summary = "\n".join(
        f"  {'✅' if i < success_count else '❌'} `{lnk.get('quality','?')}` — {lnk.get('size','?')}`"
        for i, lnk in enumerate(download_links)
    )

    if LEECH_GROUP:
        try:
            await client.send_message(
                chat_id=LEECH_GROUP,
                text=(
                    f"✅ **Manual Leech Done!**\n\n"
                    f"🎬 `{title}`\n\n"
                    f"{links_summary}\n\n"
                    f"⏱️ Total: `{format_eta(total_elapsed)}`\n"
                    f"📢 Media auto leech channel mein copy ho gayi (no forward tag)"
                ),
            )
        except Exception as e:
            log.warning(f"Leech group summary failed: {e}")

    return True


# ─── Main Loop ────────────────────────────────────────────────────────────────

async def auto_leech_loop(client: Client):
    """
    Background task — har POLL_INTERVAL seconds mein API call karta hai.
    Bot ke fully start hone ka wait karta hai pehle.
    """
    log.info(f"🤖 AutoLeech loop started — polling every {POLL_INTERVAL // 60} min")
    log.info(f"🏠 Leech Group (DL+UL): {LEECH_GROUP}")
    log.info(f"📢 Auto Leech Channel (final media copy): {AUTO_LEECH_CHANNEL}")

    if not LEECH_GROUP:
        log.error("❌ LEECH_GROUP env variable set nahi hai! AutoLeech kaam nahi karega.")
        return

    # Bot ke start hone ka wait
    while not client.is_connected:
        log.info("⏳ Bot abhi start nahi hua — 2 sec wait...")
        await asyncio.sleep(2)
    log.info("✅ Bot connected — AutoLeech ready!")

    log.info("📋 Duplicate check: MongoDB se hoga (slug-based)")

    while True:
        try:
            log.info("🔄 ExtraFlix scrape kar raha hoon...")
            new_posts = await fetch_new_posts()

            if new_posts:
                for post in new_posts:
                    await process_post(client, post)
            else:
                log.info("Koi naya post nahi mila")

        except Exception as e:
            log.exception(f"AutoLeech loop error (continuing): {e}")

        log.info(f"💤 Next poll in {POLL_INTERVAL // 60} min...")
        await asyncio.sleep(POLL_INTERVAL)
