# -*- coding: utf-8 -*-
# YouTube audio -> M4A -> Google Drive
# Clean version aligned with main.yml:
# - uses mweb -> web
# - no static po_token.txt
# - uses bgutil-ytdlp-pot-provider (script mode)
# - supports multiple cookie sets
# - uploads output to Google Drive

import json
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    os.environ["PYTHONUNBUFFERED"] = "1"

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
OUT_DIR = DATA_DIR / "audio"
LINKS = DATA_DIR / "links.txt"
DALAY = DATA_DIR / "dalay.txt"
COOKIES_MULTI = DATA_DIR / "cookies_multi.txt"

SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "5"))
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "40"))

BGUTIL_SERVER_HOME = os.environ.get(
    "BGUTIL_SERVER_HOME",
    str(Path.home() / "bgutil-ytdlp-pot-provider" / "server"),
).strip()

SCOPES = ["https://www.googleapis.com/auth/drive"]

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

if not LINKS.exists():
    LINKS.write_text("", encoding="utf-8")

if not DALAY.exists():
    DALAY.write_text("", encoding="utf-8")


def read_lines_clean(path: Path) -> List[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    lines = [x.strip() for x in lines]
    return [x for x in lines if x and not x.startswith("#")]


def resolve_ffmpeg_dir() -> Optional[str]:
    ffmpeg_bin = shutil.which("ffmpeg")
    ffprobe_bin = shutil.which("ffprobe")
    if ffmpeg_bin and ffprobe_bin:
        ff_dir = str(Path(ffmpeg_bin).parent)
        print(f"[ffmpeg] using: {ff_dir}")
        return ff_dir
    print("[ffmpeg] not found in PATH")
    return None


FFMPEG_DIR = resolve_ffmpeg_dir()

import yt_dlp
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload


def _json_cookie_to_netscape_lines(js_text: str):
    try:
        data = json.loads(js_text)
        if not isinstance(data, list):
            return None
    except Exception:
        return None

    out = ["# Netscape HTTP Cookie File"]
    for c in data:
        domain = c.get("domain", "")
        path = c.get("path", "/")
        secure = "TRUE" if c.get("secure") else "FALSE"
        include_sub = "TRUE" if domain.startswith(".") else "FALSE"
        expires = str(int(c.get("expirationDate", 2147483647)))
        name = c.get("name", "")
        value = c.get("value", "")
        if not domain or not name:
            continue
        out.append("\t".join([domain, include_sub, path, secure, expires, name, value]))
    return out


def _looks_like_netscape(txt: str) -> bool:
    for ln in txt.splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        parts = ln.split("\t")
        if len(parts) == 7:
            try:
                int(parts[4])
                return True
            except Exception:
                pass
    return False


def _split_cookie_parts(raw: str) -> List[str]:
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    # split by ===== separators first
    parts = re.split(r"^\s*[=]{5,}\s*$", raw, flags=re.MULTILINE)

    final_parts: List[str] = []
    for part in parts:
        part = part.strip()
        if not part:
            continue

        # also split multiple Netscape blocks inside same file
        lines = part.splitlines()
        current: List[str] = []
        for ln in lines:
            if ln.strip().startswith("# Netscape HTTP Cookie File") and current:
                candidate = "\n".join(current).strip()
                if candidate:
                    final_parts.append(candidate)
                current = [ln]
            else:
                current.append(ln)

        if current:
            candidate = "\n".join(current).strip()
            if candidate:
                final_parts.append(candidate)

    return final_parts


def validate_cookie_file(path: Path) -> Tuple[bool, set]:
    txt = path.read_text(encoding="utf-8", errors="ignore")
    names = set()

    for ln in txt.splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        parts = ln.split("\t")
        if len(parts) == 7:
            names.add(parts[5])

    needed = {"SAPISID", "__Secure-3PSID", "__Secure-3PAPISID"}
    has_any = bool(needed & names) or ("SID" in names and "HSID" in names)
    missing = set() if has_any else needed
    return has_any, missing


def prepare_cookie_files(cookies_multi_path: Path) -> List[str]:
    if not cookies_multi_path.exists():
        print("[cookies] no cookies_multi.txt")
        return []

    raw = cookies_multi_path.read_text(encoding="utf-8", errors="ignore")
    parts = _split_cookie_parts(raw)

    tmp_root = Path(tempfile.mkdtemp(prefix="cookies_sets_"))
    cookie_files: List[str] = []

    for idx, part in enumerate(parts):
        content = part.strip()
        if not content:
            continue

        if not _looks_like_netscape(content):
            converted = _json_cookie_to_netscape_lines(content)
            if converted:
                content = "\n".join(converted)

        file_path = tmp_root / f"ck_{idx}.txt"
        file_path.write_text(content + ("\n" if not content.endswith("\n") else ""), encoding="utf-8")

        has_lines = any(ln.strip() and not ln.strip().startswith("#") for ln in content.splitlines())
        ok, missing = validate_cookie_file(file_path)

        if has_lines and ok:
            cookie_files.append(str(file_path))
        else:
            print(f"[cookies] skip set #{idx}, missing login keys: {sorted(missing)}")

    print(f"[cookies] valid sets: {len(cookie_files)}")
    return cookie_files


def load_oauth_from_env() -> Optional[Credentials]:
    tok = os.environ.get("GDRIVE_OAUTH_TOKEN_JSON", "").strip()
    if not tok:
        return None
    try:
        info = json.loads(tok)
        return Credentials.from_authorized_user_info(info, SCOPES)
    except Exception as exc:
        print(f"[drive] invalid oauth token json: {exc}")
        return None


def load_sa_credentials() -> Optional[service_account.Credentials]:
    sa_json_text = os.environ.get("GDRIVE_SA_JSON", "").strip()
    if not sa_json_text:
        return None
    try:
        info = json.loads(sa_json_text)
        if info.get("type") == "service_account":
            return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    except Exception as exc:
        print(f"[drive] service account error: {exc}")
    return None


def init_drive_service():
    creds = load_oauth_from_env()
    if creds:
        print("[drive] using oauth token")
        return build("drive", "v3", credentials=creds)

    sa = load_sa_credentials()
    if sa:
        print("[drive] using service account")
        return build("drive", "v3", credentials=sa)

    print("[drive] no drive credentials found, upload disabled")
    return None


def ensure_folder_by_id(service, folder_id: str) -> Optional[str]:
    if not service or not folder_id:
        print("[drive] missing service or folder id")
        return None
    try:
        meta = service.files().get(
            fileId=folder_id,
            fields="id,name",
            supportsAllDrives=True,
        ).execute()
        print(f"[drive] target folder: {meta.get('name')} ({meta.get('id')})")
        return meta["id"]
    except HttpError as exc:
        print(f"[drive] cannot access folder '{folder_id}': {exc}")
        return None


def drive_upload_file(service, file_path: Path, folder_id: str):
    media = MediaFileUpload(str(file_path), mimetype="audio/mp4", resumable=True)
    body = {"name": file_path.name, "parents": [folder_id]}
    created = service.files().create(
        body=body,
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return created["id"], "created"


def detect_js_runtimes():
    runtimes = {}
    deno = shutil.which("deno")
    node = shutil.which("node")
    bun = shutil.which("bun")

    if deno:
        runtimes["deno"] = {"path": deno}
    if node:
        runtimes["node"] = {"path": node}
    if bun:
        runtimes["bun"] = {"path": bun}

    if runtimes:
        print("[js] runtimes:", ", ".join(f"{k}={v['path']}" for k, v in runtimes.items()))
    else:
        print("[js] no js runtime found")
    return runtimes


BASE_YDL_OPTS = {
    "format": (
        "bestaudio[protocol!=m3u8_native][protocol!=m3u8]/"
        "bestaudio/"
        "best"
    ),
    "merge_output_format": "m4a",
    "outtmpl": str(OUT_DIR / "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": False,
    "nocheckcertificate": True,
    "cachedir": False,
    "retries": 10,
    "fragment_retries": 10,
    "concurrent_fragment_downloads": 1,
    "force_ipv4": True,
    "http_headers": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.youtube.com/",
        "Origin": "https://www.youtube.com",
    },
    "remote_components": {"ejs:github"},
    "postprocessors": [
        {
            "key": "FFmpegExtractAudio",
            "preferredcodec": "m4a",
        }
    ],
}

_js_runtimes = detect_js_runtimes()
if _js_runtimes:
    BASE_YDL_OPTS["js_runtimes"] = _js_runtimes

if FFMPEG_DIR:
    BASE_YDL_OPTS["ffmpeg_location"] = FFMPEG_DIR

last_good_cookie_idx = 0


def _ydl_opts_with_client(base_opts: dict, player_clients: List[str], cookiefile: Optional[str]):
    opts = dict(base_opts)

    extractor_args = {
        "youtube": {
            "player_client": player_clients,
            "skip": ["hls"],
        }
    }

    if BGUTIL_SERVER_HOME:
        extractor_args["youtubepot-bgutilscript"] = {
            "server_home": [BGUTIL_SERVER_HOME]
        }
        print(f"[pot] bgutil server_home={BGUTIL_SERVER_HOME}")

    opts["extractor_args"] = extractor_args

    if cookiefile:
        opts["cookiefile"] = cookiefile
    else:
        opts.pop("cookiefile", None)

    return opts


def list_audio_files() -> set:
    files = set()
    for pattern in ("*.m4a", "*.mp4", "*.webm", "*.mp3", "*.m4b"):
        files |= set(OUT_DIR.glob(pattern))
    return files


def try_download_with_cookies(url: str) -> Tuple[bool, Optional[str], Optional[Path]]:
    global last_good_cookie_idx

    cookie_files = prepare_cookie_files(COOKIES_MULTI)

    if cookie_files:
        order = list(range(len(cookie_files)))
        if last_good_cookie_idx < len(cookie_files):
            order = list(range(last_good_cookie_idx, len(cookie_files))) + list(range(0, last_good_cookie_idx))
        order = order + [None]
    else:
        order = [None]

    last_err = "download failed with all cookie/client combinations"

    for ck_idx in order:
        cookiefile = cookie_files[ck_idx] if ck_idx is not None else None

        if cookiefile:
            print(f"   -> trying cookie set #{ck_idx}")
        else:
            print("   -> trying without cookie")

        for clients in (["mweb"], ["web"]):
            client_name = ",".join(clients)
            try:
                print(f"      [client] {client_name}")
                ydl_opts = _ydl_opts_with_client(BASE_YDL_OPTS, clients, cookiefile)

                before = list_audio_files()

                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    predicted = Path(ydl.prepare_filename(info))

                    for existing in before:
                        if existing.stem == predicted.stem:
                            print(f"   -> already exists: {existing.name}")
                            return True, None, None

                    ydl.download([url])

                after = list_audio_files()
                new_files = sorted(after - before, key=lambda p: p.stat().st_mtime, reverse=True)

                if new_files:
                    if ck_idx is not None:
                        last_good_cookie_idx = ck_idx
                    return True, None, new_files[0]

                return True, "no new file created", None

            except Exception as exc:
                last_err = str(exc)
                print(f"      [fail {client_name}] {last_err}")

    return False, last_err, None


def main():
    all_links = read_lines_clean(LINKS)
    done_links = set(read_lines_clean(DALAY))
    run_list = [url for url in all_links if url not in done_links][:MAX_PER_RUN]

    print(f"total={len(all_links)} done={len(done_links)} this_run={len(run_list)}")

    drive_service = init_drive_service()
    folder_id = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
    resolved_folder_id = ensure_folder_by_id(drive_service, folder_id) if drive_service else None

    success = []
    failed = []
    uploaded = []

    if not run_list:
        print("no new links to process")
        return

    for i, url in enumerate(run_list, 1):
        print(f"\n[{i}/{len(run_list)}] processing: {url}")
        ok, err, fpath = try_download_with_cookies(url)

        if ok:
            print(" -> download OK")
            task_successful = True

            if fpath and fpath.exists():
                if drive_service and resolved_folder_id:
                    try:
                        file_id, action = drive_upload_file(drive_service, fpath, resolved_folder_id)
                        uploaded.append((fpath.name, action, file_id))
                        print(f"    [drive] uploaded: {fpath.name}")

                        try:
                            os.remove(fpath)
                            print(f"    [local] removed: {fpath.name}")
                        except OSError as exc:
                            print(f"    [local] remove failed {fpath.name}: {exc}")

                    except Exception as exc:
                        print(f"    [drive] upload failed: {exc}")
                        failed.append((url, f"upload failed: {exc}"))
                        task_successful = False
                else:
                    print(f"    [local] keep file: {fpath.name}")
            else:
                print("    -> no new file to upload")

            if task_successful:
                success.append(url)
        else:
            failed.append((url, err))
            print(f" -> FAILED: {err}")

        if i < len(run_list):
            print(f"   sleep {SLEEP_SECONDS}s...")
            time.sleep(SLEEP_SECONDS)

    if success:
        print(f"\nupdating dalay.txt with {len(success)} success links...")
        existing_done = set(read_lines_clean(DALAY))
        all_done = existing_done.union(set(success))
        DALAY.write_text("\n".join(sorted(all_done)) + "\n", encoding="utf-8")
        print(" -> dalay.txt updated")

    if drive_service and resolved_folder_id and DALAY.exists():
        try:
            drive_upload_file(drive_service, DALAY, resolved_folder_id)
            print("[drive] uploaded latest dalay.txt")
        except Exception as exc:
            print(f"[drive] dalay.txt upload failed: {exc}")

    print("\n=== SUMMARY ===")
    print(f"success={len(success)} failed={len(failed)}")

    if uploaded:
        print("uploaded files:")
        for name, action, _ in uploaded:
            print(f" - {name} ({action})")

    if failed:
        print("\nfailures:")
        for u, e in failed:
            print(f"- {u}\n  reason: {e}\n")


if __name__ == "__main__":
    main()
