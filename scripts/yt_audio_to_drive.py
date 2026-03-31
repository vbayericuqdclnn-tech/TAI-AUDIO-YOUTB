# -*- coding: utf-8 -*-
# YouTube audio -> M4A -> Google Drive
# FIX:
# - bỏ android / web_embedded / web_safari
# - bỏ po_token.txt tĩnh
# - dùng mweb -> web
# - tích hợp bgutil POT provider (script mode) cho yt-dlp
# - nới format để tránh chết vì map format thay đổi
# - giữ tránh HLS/m3u8 để giảm lỗi fragment/CDN

import os
import sys
import re
import json
import time
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Tuple, List

# --- CẤU HÌNH ---
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

# bgutil POT provider - script mode
BGUTIL_SERVER_HOME = Path(
    os.environ.get(
        "BGUTIL_SERVER_HOME",
        str(Path.home() / "bgutil-ytdlp-pot-provider" / "server"),
    )
).expanduser()

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

if not LINKS.exists():
    LINKS.write_text("", encoding="utf-8")
if not DALAY.exists():
    DALAY.write_text("", encoding="utf-8")

SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "8"))
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "40"))

# --- CÁC HÀM TIỆN ÍCH ---

def _resolve_ffmpeg_dir() -> Optional[str]:
    ffmpeg_bin = shutil.which("ffmpeg")
    ffprobe_bin = shutil.which("ffprobe")
    if ffmpeg_bin and ffprobe_bin:
        p1 = Path(ffmpeg_bin).parent
        print(f"[ffmpeg] Dùng system ffmpeg/ffprobe: {p1}")
        return str(p1)
    print("[ffmpeg] Không thấy ffmpeg/ffprobe trong PATH.")
    return None

FFMPEG_DIR = _resolve_ffmpeg_dir()

import yt_dlp
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

SCOPES = ["https://www.googleapis.com/auth/drive"]

def read_lines_clean(p: Path) -> List[str]:
    if not p.exists():
        return []
    lines = [ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines()]
    return [ln for ln in lines if ln and not ln.startswith("#")]

# --- XỬ LÝ COOKIE ---

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
    """
    Hỗ trợ 2 kiểu:
    1) nhiều bộ cookie ngăn cách bằng dòng =====
    2) nhiều block Netscape trong cùng 1 file
    """
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    manual_parts = re.split(r"^\s*[=]{5,}\s*$", raw, flags=re.MULTILINE)

    final_parts: List[str] = []
    for part in manual_parts:
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

def validate_cookie_file(path: Path):
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
        print("[cookies] Không có cookies_multi.txt")
        return []

    raw = cookies_multi_path.read_text(encoding="utf-8", errors="ignore")
    parts = _split_cookie_parts(raw)

    cookie_files: List[str] = []
    tmp_root = Path(tempfile.mkdtemp(prefix="cookies_sets_"))

    for idx, part in enumerate(parts):
        content = part.strip()
        if not content:
            continue

        if not _looks_like_netscape(content):
            lines = _json_cookie_to_netscape_lines(content)
            if lines:
                content = "\n".join(lines)

        f = tmp_root / f"ck_{idx}.txt"
        f.write_text(content + ("\n" if not content.endswith("\n") else ""), encoding="utf-8")

        has_lines = any((ln.strip() and not ln.strip().startswith("#")) for ln in content.splitlines())
        ok, missing = validate_cookie_file(f)

        if has_lines and ok:
            cookie_files.append(str(f))
        else:
            print(f"[WARN] Bộ cookie #{idx} bỏ qua do thiếu khoá đăng nhập: {sorted(missing)}")

    print(f"[cookies] Số bộ cookie hợp lệ: {len(cookie_files)}")
    return cookie_files

# --- XÁC THỰC GOOGLE DRIVE ---

def load_oauth_from_env() -> Optional[Credentials]:
    tok = os.environ.get("GDRIVE_OAUTH_TOKEN_JSON", "").strip()
    if not tok:
        return None
    try:
        info = json.loads(tok)
        return Credentials.from_authorized_user_info(info, SCOPES)
    except Exception as e:
        print(f"[Drive] OAuth token JSON không hợp lệ: {e}")
        return None

def load_sa_credentials() -> Optional[service_account.Credentials]:
    sa_json_text = os.environ.get("GDRIVE_SA_JSON", "").strip()
    if not sa_json_text:
        return None
    try:
        info = json.loads(sa_json_text)
        if info.get("type") == "service_account":
            return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    except Exception as e:
        print(f"[Drive] Service Account lỗi: {e}")
    return None

def init_drive_service():
    """
    Chỉ dùng credentials từ biến môi trường, không mở browser/local auth flow.
    """
    creds = load_oauth_from_env()
    if creds:
        print("[Drive] Dùng OAuth token từ GDRIVE_OAUTH_TOKEN_JSON.")
        return build("drive", "v3", credentials=creds)

    sa = load_sa_credentials()
    if sa:
        print("[Drive] Dùng Service Account từ GDRIVE_SA_JSON.")
        return build("drive", "v3", credentials=sa)

    print("[Drive] Không tìm thấy GDRIVE_OAUTH_TOKEN_JSON hoặc GDRIVE_SA_JSON. Bỏ qua upload.")
    return None

def ensure_folder_by_id(service, folder_id: str) -> Optional[str]:
    if not service or not folder_id:
        print("[Drive] Thiếu service hoặc Folder ID.")
        return None
    try:
        meta = service.files().get(
            fileId=folder_id,
            fields="id,name",
            supportsAllDrives=True
        ).execute()
        print(f"[Drive] Dùng folder: {meta.get('name')} ({meta.get('id')})")
        return meta["id"]
    except HttpError as e:
        print(f"[Drive] Không truy cập được Folder ID '{folder_id}': {e}")
        return None

def drive_upload_file(service, file_path: Path, folder_id: str):
    name = file_path.name
    media = MediaFileUpload(str(file_path), mimetype="audio/mp4", resumable=True)
    body = {"name": name, "parents": [folder_id]}
    created = service.files().create(
        body=body,
        media_body=media,
        fields="id",
        supportsAllDrives=True
    ).execute()
    return created["id"], "created"

# --- LOGIC YT-DLP ---

def _detect_js_runtimes():
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
        print("[EJS] JS runtimes detected:", ", ".join(f"{k}={v.get('path')}" for k, v in runtimes.items()))
    else:
        print("[EJS] WARN: Không thấy deno/node/bun trong PATH. EJS có thể fail.")
    return runtimes

BASE_YDL_OPTS = {
    # Nới format để đỡ chết khi YouTube đổi map format
    # Vẫn tránh HLS/m3u8 nếu có thể
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

    # Cứ tải xong thì ép ra M4A
    "postprocessors": [
        {
            "key": "FFmpegExtractAudio",
            "preferredcodec": "m4a",
        }
    ],

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

    # cho yt-dlp tự kéo EJS components nếu cần
    "remote_components": {"ejs:github"},
}

_js = _detect_js_runtimes()
if _js:
    BASE_YDL_OPTS["js_runtimes"] = _js

if FFMPEG_DIR:
    BASE_YDL_OPTS["ffmpeg_location"] = FFMPEG_DIR

last_good_cookie_idx = 0

def _ydl_opts_with_client(base_opts: dict, player_clients: list, cookiefile: Optional[str]):
    opts = dict(base_opts)

    extractor_args = {
        "youtube": {
            # chỉ dùng mweb -> web
            "player_client": player_clients,
            # bỏ HLS để giảm fragment/CDN lỗi
            "skip": ["hls"],
        }
    }

    # bgutil POT provider - script mode
    # nếu workflow setup đúng, plugin sẽ dùng server_home này để mint POT tự động
    if BGUTIL_SERVER_HOME.exists():
        extractor_args["youtubepot-bgutilscript"] = {
            "server_home": [str(BGUTIL_SERVER_HOME)]
        }
        print(f"[POT] Dùng bgutil script provider: {BGUTIL_SERVER_HOME}")
    else:
        print(f"[POT] WARN: Không thấy BGUTIL_SERVER_HOME: {BGUTIL_SERVER_HOME}")

    opts["extractor_args"] = extractor_args

    if cookiefile:
        opts["cookiefile"] = cookiefile
    else:
        opts.pop("cookiefile", None)

    return opts

def _list_audio_files() -> set:
    exts = ("*.m4a", "*.mp4", "*.webm", "*.mp3", "*.m4b")
    files = set()
    for pat in exts:
        files |= set(OUT_DIR.glob(pat))
    return files

def try_download_with_cookies(url: str) -> Tuple[bool, Optional[str], Optional[Path]]:
    """
    - Thử lần lượt các bộ cookie trong cookies_multi.txt
    - Ưu tiên bộ vừa thành công gần nhất
    - Cuối cùng fallback 1 lần: không cookie
    - Chỉ thử mweb -> web
    """
    global last_good_cookie_idx

    COOKIE_FILES = prepare_cookie_files(COOKIES_MULTI)
    if COOKIE_FILES:
        base_order = list(range(len(COOKIE_FILES)))
        if last_good_cookie_idx < len(COOKIE_FILES):
            base_order = list(range(last_good_cookie_idx, len(COOKIE_FILES))) + list(range(0, last_good_cookie_idx))
        order = base_order + [None]
    else:
        order = [None]

    last_err = "Không thể tải về với tất cả các tùy chọn."

    for ck_idx in order:
        cookiefile = COOKIE_FILES[ck_idx] if ck_idx is not None else None

        if cookiefile:
            print(f"   -> Thử cookie set #{ck_idx}")
        else:
            print("   -> Thử không dùng cookie")

        # Chỉ còn mweb rồi web
        plans = [["mweb"], ["web"]]

        for pcs in plans:
            client_name = ",".join(pcs)
            try:
                print(f"      [client] {client_name}")
                ydl_opts = _ydl_opts_with_client(BASE_YDL_OPTS, pcs, cookiefile)

                before = _list_audio_files()

                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info_dict = ydl.extract_info(url, download=False)
                    filename = Path(ydl.prepare_filename(info_dict))

                    # Nếu file cùng stem đã có rồi thì bỏ qua
                    for existing in before:
                        if existing.stem == filename.stem:
                            print(f"   -> File đã tồn tại: '{existing.name}'. Bỏ qua tải về.")
                            return True, None, None

                    ydl.download([url])

                after = _list_audio_files()
                new_files = sorted(list(after - before), key=lambda p: p.stat().st_mtime, reverse=True)

                if new_files:
                    if ck_idx is not None:
                        last_good_cookie_idx = ck_idx
                    return True, None, new_files[0]

                return True, "Không có file mới được tạo (có thể đã tồn tại)", None

            except Exception as e:
                last_err = str(e)
                print(f"      [FAIL {client_name}] {last_err}")
                continue

    return False, last_err, None

# --- CHUẨN BỊ CHẠY ---

all_links = read_lines_clean(LINKS)
done_links = set(read_lines_clean(DALAY))
run_list = [url for url in all_links if url not in done_links][:MAX_PER_RUN]

print(f"Tổng: {len(all_links)} | Đã làm: {len(done_links)} | Sẽ xử lý trong lần chạy này: {len(run_list)}")
print(f"[POT] BGUTIL_SERVER_HOME = {BGUTIL_SERVER_HOME}")

GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
drive_service = init_drive_service()
resolved_folder_id = ensure_folder_by_id(drive_service, GDRIVE_FOLDER_ID) if drive_service else None

success, failed, uploaded = [], [], []

if not run_list:
    print("Không có link mới để tải.")

# --- VÒNG LẶP XỬ LÝ CHÍNH ---

for i, url in enumerate(run_list, 1):
    print(f"\n[{i}/{len(run_list)}] Đang xử lý: {url}")
    ok, err, fpath = try_download_with_cookies(url)

    if ok:
        print(" -> Tải về OK.")
        task_successful = True

        if fpath and fpath.exists():
            if drive_service and resolved_folder_id:
                try:
                    fid, action = drive_upload_file(drive_service, fpath, resolved_folder_id)
                    uploaded.append((fpath.name, action, fid))
                    print(f"    [Drive] Upload thành công: {fpath.name}")

                    try:
                        os.remove(fpath)
                        print(f"    [Local] Đã xóa file: {fpath.name}")
                    except OSError as oe:
                        print(f"    [Local] Lỗi khi xóa file {fpath.name}: {oe}")

                except Exception as ue:
                    print(f"    [Drive] Upload lỗi: {ue}")
                    failed.append((url, f"Upload failed: {ue}"))
                    task_successful = False
            else:
                print(f"    [Local] Giữ lại file (không cấu hình Drive): {fpath.name}")
        else:
            print("    -> Không có file mới để upload (có thể đã tồn tại từ trước).")

        if task_successful:
            success.append(url)
    else:
        failed.append((url, err))
        print(f" -> Tải về THẤT BẠI: {err}")

    if i < len(run_list):
        print(f"   Nghỉ {SLEEP_SECONDS} giây...")
        time.sleep(SLEEP_SECONDS)

# --- CẬP NHẬT KẾT QUẢ ---

if success:
    print(f"\nCập nhật dalay.txt với {len(success)} link thành công...")
    try:
        existing_done_links = set(read_lines_clean(DALAY))
        all_done_links = existing_done_links.union(set(success))
        DALAY.write_text("\n".join(sorted(list(all_done_links))) + "\n", encoding="utf-8")
        print(" -> Cập nhật dalay.txt thành công.")
    except Exception as e:
        print(f" [LỖI] Cập nhật dalay.txt thất bại: {e}")

if drive_service and resolved_folder_id and DALAY.exists():
    try:
        drive_upload_file(service=drive_service, file_path=DALAY, folder_id=resolved_folder_id)
        print("[Drive] Đã upload phiên bản mới của dalay.txt")
    except Exception as e:
        print(f"[Drive] Upload dalay.txt lỗi: {e}")

print("\n=== TỔNG KẾT ===")
print(f"Thành công: {len(success)} | Thất bại: {len(failed)}")

if uploaded:
    print("Đã upload lên Drive:")
    for n, action, fid in uploaded:
        print(f" - {n} ({action})")

if failed:
    print("\nDanh sách lỗi:")
    for u, e in failed:
        print(f"- {u}\n  Lý do: {e}\n")
