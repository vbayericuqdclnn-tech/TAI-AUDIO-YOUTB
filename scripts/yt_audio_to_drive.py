# -*- coding: utf-8 -*-
# GitHub Actions friendly YouTube audio -> M4A -> Google Drive
# Strategy:
# 1) Try guest session first for public videos
# 2) Fallback to cookies only if needed
# 3) Use yt-dlp CLI (not Python API) so plugin loading/debug matches documented CLI behavior
# 4) Prefer mweb + PO Token provider, then web, then web_safari fallback
# 5) Keep delays between videos to reduce rate-limit pressure

import json
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

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

YT_DLP_BIN = os.environ.get("YT_DLP_BIN", "yt-dlp")
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "40"))
SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "7"))
TOKEN_TTL = os.environ.get("TOKEN_TTL", "6").strip()
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

INVALID_COOKIE_FILES = set()

ATTEMPTS = [
    {"label": "guest/mweb", "client": "mweb", "use_cookie": False, "skip_hls": True},
    {"label": "guest/web", "client": "web", "use_cookie": False, "skip_hls": True},
    {"label": "guest/web_safari", "client": "web_safari", "use_cookie": False, "skip_hls": False},
    {"label": "cookie/mweb", "client": "mweb", "use_cookie": True, "skip_hls": True},
    {"label": "cookie/web", "client": "web", "use_cookie": True, "skip_hls": True},
    {"label": "cookie/web_safari", "client": "web_safari", "use_cookie": True, "skip_hls": False},
]


def read_lines_clean(path: Path) -> List[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    lines = [x.strip() for x in lines]
    return [x for x in lines if x and not x.startswith("#")]


def newest_audio_file(before: set) -> Optional[Path]:
    current = set()
    for pattern in ("*.m4a", "*.mp3", "*.mp4", "*.webm", "*.m4b"):
        current |= set(OUT_DIR.glob(pattern))
    new_files = sorted(list(current - before), key=lambda p: p.stat().st_mtime, reverse=True)
    return new_files[0] if new_files else None


def list_audio_files() -> set:
    files = set()
    for pattern in ("*.m4a", "*.mp3", "*.mp4", "*.webm", "*.m4b"):
        files |= set(OUT_DIR.glob(pattern))
    return files


def guess_mimetype(file_path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(file_path))
    return mime or "application/octet-stream"


def resolve_ffmpeg() -> None:
    ffmpeg_bin = shutil.which("ffmpeg")
    ffprobe_bin = shutil.which("ffprobe")
    print(f"[env] yt-dlp      = {YT_DLP_BIN}")
    print(f"[env] ffmpeg      = {ffmpeg_bin}")
    print(f"[env] ffprobe     = {ffprobe_bin}")
    print(f"[env] bgutil home = {BGUTIL_SERVER_HOME}")
    print(f"[env] token ttl   = {TOKEN_TTL}")
    if not ffmpeg_bin or not ffprobe_bin:
        raise RuntimeError("ffmpeg/ffprobe not found in PATH")


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
    parts = re.split(r"^\s*[=]{5,}\s*$", raw, flags=re.MULTILINE)

    final_parts: List[str] = []
    for part in parts:
        part = part.strip()
        if not part:
            continue

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
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    sa = load_sa_credentials()
    if sa:
        print("[drive] using service account")
        return build("drive", "v3", credentials=sa, cache_discovery=False)

    print("[drive] no drive credentials found; upload disabled")
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
    media = MediaFileUpload(str(file_path), mimetype=guess_mimetype(file_path), resumable=True)
    body = {"name": file_path.name, "parents": [folder_id]}
    created = service.files().create(
        body=body,
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return created["id"], "created"


def build_cmd(url: str, client: str, skip_hls: bool, cookiefile: Optional[str]) -> List[str]:
    youtube_args = f"youtube:player_client={client}"
    if skip_hls:
        youtube_args += ";skip=hls"

    cmd = [
        YT_DLP_BIN,
        "-v",
        "--ignore-config",
        "--no-playlist",
        "--newline",
        "--retries", "10",
        "--fragment-retries", "10",
        "--concurrent-fragments", "1",
        "--output", str(OUT_DIR / "%(title).180B [%(id)s].%(ext)s"),
        "--format", "bestaudio[ext=m4a]/bestaudio/best",
        "--extract-audio",
        "--audio-format", "m4a",
        "--audio-quality", "0",
        "--print", "after_move:__FINAL_FILE__=%(filepath)s",
        "--add-headers", "User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "--add-headers", "Accept-Language:en-US,en;q=0.9",
        "--extractor-args", youtube_args,
        "--extractor-args", f"youtubepot-bgutilscript:server_home={BGUTIL_SERVER_HOME}",
        url,
    ]

    if cookiefile:
        cmd[1:1] = ["--cookies", cookiefile]

    return cmd


def run_yt_dlp_once(url: str, label: str, client: str, skip_hls: bool, cookiefile: Optional[str]) -> Tuple[bool, str, Optional[Path]]:
    before = list_audio_files()
    cmd = build_cmd(url=url, client=client, skip_hls=skip_hls, cookiefile=cookiefile)

    print(f"      [run] {label}")
    print("      [cmd] " + " ".join(shlex_quote(x) for x in cmd[:-1]) + " <URL>")

    env = os.environ.copy()
    env["TOKEN_TTL"] = TOKEN_TTL

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )

    output = proc.stdout or ""
    print(output)

    if cookiefile and "The provided YouTube account cookies are no longer valid" in output:
        INVALID_COOKIE_FILES.add(cookiefile)

    final_path = None
    for line in output.splitlines():
        if line.startswith("__FINAL_FILE__="):
            maybe = line.split("=", 1)[1].strip()
            if maybe:
                p = Path(maybe)
                if p.exists():
                    final_path = p
                    break

    if final_path is None:
        final_path = newest_audio_file(before)

    if proc.returncode == 0 and final_path and final_path.exists():
        return True, output, final_path

    err = last_meaningful_line(output) or f"yt-dlp exited with code {proc.returncode}"
    return False, err, None


def shlex_quote(s: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_./:=+-]+", s):
        return s
    return "'" + s.replace("'", "'\"'\"'") + "'"


def last_meaningful_line(output: str) -> str:
    lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    for ln in reversed(lines):
        if ln.startswith("ERROR:"):
            return ln
    return lines[-1] if lines else ""


def try_download(url: str, cookie_files: List[str]) -> Tuple[bool, str, Optional[Path]]:
    cookies_available = [x for x in cookie_files if x not in INVALID_COOKIE_FILES]
    cookie_order = cookies_available if cookies_available else []

    for attempt in ATTEMPTS:
        if attempt["use_cookie"]:
            if not cookie_order:
                continue
            for idx, cookiefile in enumerate(cookie_order):
                print(f"   -> cookie set #{idx} | {attempt['label']}")
                ok, info, path = run_yt_dlp_once(
                    url=url,
                    label=attempt["label"],
                    client=attempt["client"],
                    skip_hls=attempt["skip_hls"],
                    cookiefile=cookiefile,
                )
                if ok:
                    return True, "", path
                time.sleep(1)
        else:
            print(f"   -> {attempt['label']}")
            ok, info, path = run_yt_dlp_once(
                url=url,
                label=attempt["label"],
                client=attempt["client"],
                skip_hls=attempt["skip_hls"],
                cookiefile=None,
            )
            if ok:
                return True, "", path
            time.sleep(1)

    if INVALID_COOKIE_FILES:
        return False, "all attempts failed; at least one cookie set was reported invalid/rotated", None
    return False, "all attempts failed", None


def main():
    resolve_ffmpeg()

    all_links = read_lines_clean(LINKS)
    done_links = set(read_lines_clean(DALAY))
    run_list = [url for url in all_links if url not in done_links][:MAX_PER_RUN]
    cookie_files = prepare_cookie_files(COOKIES_MULTI)

    print(f"[queue] total={len(all_links)} done={len(done_links)} this_run={len(run_list)}")
    print(f"[cookies] usable={len([x for x in cookie_files if x not in INVALID_COOKIE_FILES])}")

    drive_service = init_drive_service()
    folder_id = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
    resolved_folder_id = ensure_folder_by_id(drive_service, folder_id) if drive_service else None

    success = []
    failed = []
    uploaded = []

    if not run_list:
        print("[queue] no new links")
        return

    for i, url in enumerate(run_list, 1):
        print(f"\n[{i}/{len(run_list)}] processing: {url}")
        ok, err, file_path = try_download(url, cookie_files)

        if ok:
            print(" -> download OK")
            task_successful = True

            if file_path and file_path.exists():
                if drive_service and resolved_folder_id:
                    try:
                        file_id, action = drive_upload_file(drive_service, file_path, resolved_folder_id)
                        uploaded.append((file_path.name, action, file_id))
                        print(f"    [drive] uploaded: {file_path.name}")

                        try:
                            os.remove(file_path)
                            print(f"    [local] removed: {file_path.name}")
                        except OSError as exc:
                            print(f"    [local] remove failed: {exc}")

                    except Exception as exc:
                        print(f"    [drive] upload failed: {exc}")
                        failed.append((url, f"upload failed: {exc}"))
                        task_successful = False
                else:
                    print(f"    [local] keeping file: {file_path.name}")
            else:
                print("    [warn] no output file found after success")

            if task_successful:
                success.append(url)
        else:
            failed.append((url, err))
            print(f" -> FAILED: {err}")

        if i < len(run_list):
            print(f"   sleep {SLEEP_SECONDS}s...")
            time.sleep(SLEEP_SECONDS)

    if success:
        existing_done = set(read_lines_clean(DALAY))
        all_done = existing_done.union(set(success))
        DALAY.write_text("\n".join(sorted(all_done)) + "\n", encoding="utf-8")
        print(f"[dalay] updated with {len(success)} success links")

    if drive_service and resolved_folder_id and DALAY.exists():
        try:
            drive_upload_file(drive_service, DALAY, resolved_folder_id)
            print("[drive] uploaded latest dalay.txt")
        except Exception as exc:
            print(f"[drive] dalay upload failed: {exc}")

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
