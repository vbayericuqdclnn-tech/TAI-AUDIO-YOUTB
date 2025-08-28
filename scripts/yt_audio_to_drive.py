# -*- coding: utf-8 -*-
# YouTube audio -> M4A -> Google Drive
# - Mỗi lần chạy: 1 link (lấy dòng mới đầu tiên trong data/links.txt chưa có trong data/dalay.txt)
# - Cookie rotation (data/cookies_multi.txt, Netscape hoặc JSON; ngăn cách "=====")
# - Player client rotation: web -> web_embedded -> android (khi có cookie); android -> web -> web_embedded (khi ẩn danh)
# - PO_TOKEN tuỳ chọn (env PO_TOKEN hoặc data/po_token.txt)
# - Upload Drive: ƯU TIÊN OAuth (GDRIVE_OAUTH_TOKEN_JSON) rồi mới fallback Service Account (GDRIVE_SA_JSON)
# - Full Drive scope; tránh f-string chứa backslash
# - Sửa ffmpeg/ffprobe: chỉ set ffmpeg_location khi PATH có đủ ffmpeg & ffprobe cùng thư mục
# - Mỗi lần chạy: xử lý N link (mặc định 10, qua env MAX_PER_RUN)
# - Cookie rotation + player-client rotation; PO_TOKEN tùy chọn
# - Upload Drive: ƯU TIÊN OAuth (GDRIVE_OAUTH_TOKEN_JSON), fallback SA (GDRIVE_SA_JSON)
# - Full Drive scope; fix ffmpeg/ffprobe; unbuffered logs

import os, sys, re, json, time, shutil, tempfile
from pathlib import Path
from typing import Optional, Tuple, List

# Ép log xả theo dòng nếu môi trường cho phép
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    os.environ["PYTHONUNBUFFERED"] = "1"

# -------- Paths --------
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR  = REPO_ROOT / "data"
OUT_DIR   = DATA_DIR / "audio"
LINKS     = DATA_DIR / "links.txt"
DALAY     = DATA_DIR / "dalay.txt"
COOKIES_MULTI = DATA_DIR / "cookies_multi.txt"
PO_TOKEN_FILE = DATA_DIR / "po_token.txt"
TOKEN_STORE   = DATA_DIR / "drive_token.json"  # chỉ dùng local nếu OAuth interactive
TOKEN_STORE   = DATA_DIR / "drive_token.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "8"))
SMOKE_TEST = os.environ.get("SMOKE_TEST", "0").strip() == "1"
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "1000"))  # <<< chạy 1000 link/lần

# -------- ffmpeg/ffprobe & yt-dlp --------
def _resolve_ffmpeg_dir() -> Optional[str]:
    """Trả về thư mục chung của ffmpeg & ffprobe nếu cùng chỗ; nếu không, None để dùng PATH."""
    ffmpeg_bin  = shutil.which("ffmpeg")
    ffprobe_bin = shutil.which("ffprobe")
    if ffmpeg_bin and ffprobe_bin:
@@ -49,7 +43,6 @@ def _resolve_ffmpeg_dir() -> Optional[str]:
            return str(p1)
        print(f"[ffmpeg] ffmpeg tại {p1}, ffprobe tại {p2} -> dùng PATH, không set ffmpeg_location")
        return None
    # fallback: imageio-ffmpeg (thường chỉ có ffmpeg, không có ffprobe)
    try:
        import imageio_ffmpeg  # noqa
        alt = Path(__import__("imageio_ffmpeg").get_ffmpeg_exe()).parent  # type: ignore
@@ -60,19 +53,17 @@ def _resolve_ffmpeg_dir() -> Optional[str]:

FFMPEG_DIR = _resolve_ffmpeg_dir()

import yt_dlp  # sau khi xác định FFMPEG_DIR
import yt_dlp

# -------- Google Drive --------
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]  # full scope
SCOPES = ["https://www.googleapis.com/auth/drive"]

# -------- Utils --------
def read_lines_clean(p: Path) -> List[str]:
    if not p.exists(): return []
    lines = [ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines()]
@@ -143,19 +134,19 @@ def prepare_cookie_files(cookies_multi_path: Path) -> List[str]:
COOKIE_FILES = prepare_cookie_files(COOKIES_MULTI)
print(f"Cookies sets hợp lệ: {len(COOKIE_FILES)}" if COOKIE_FILES else "Không dùng cookies hoặc tất cả set không hợp lệ.")

# -------- Build link list: chỉ lấy 1 link --------
all_links  = read_lines_clean(LINKS)
done_links = set(read_lines_clean(DALAY))
seen, new_links = set(), []
for url in all_links:
    if url in done_links or url in seen: continue
    seen.add(url); new_links.append(url)
print(f"Tổng: {len(all_links)} | Đã làm: {len(done_links)} | Mới sẽ xử lý: {len(new_links)}")
run_list = new_links[:1]  # 1 link/run
# --- chạy 10 link / lần (hoặc MAX_PER_RUN) ---
run_list = new_links[:MAX_PER_RUN]

po_token = (os.environ.get("PO_TOKEN") or (PO_TOKEN_FILE.read_text(encoding="utf-8").strip() if PO_TOKEN_FILE.exists() else "")).strip()

# -------- Drive auth (OAuth-first) --------
def load_oauth_from_env() -> Optional[Credentials]:
    tok = os.environ.get("GDRIVE_OAUTH_TOKEN_JSON", "").strip()
    if not tok:
@@ -184,7 +175,6 @@ def load_sa_credentials() -> Optional[service_account.Credentials]:
    return None

def init_drive_service():
    # 1) Prefer OAuth (quota tài khoản của bạn)
    creds = load_oauth_from_env()
    if creds:
        try:
@@ -193,7 +183,6 @@ def init_drive_service():
        except Exception as e:
            print(f"[Drive] Không khởi tạo được Drive service (OAuth): {e}")

    # 2) Fallback Service Account
    sa = load_sa_credentials()
    if sa:
        try:
@@ -209,7 +198,6 @@ def init_drive_service():
            print(f"[Drive] Không khởi tạo được Drive service (SA): {e}")
            return None

    # 3) Local OAuth interactive (không dùng trên Actions)
    try:
        creds = None
        if TOKEN_STORE.exists():
@@ -241,12 +229,11 @@ def ensure_folder_by_id(service, folder_id: str) -> Optional[str]:
            fileId=folder_id, fields="id,name,driveId", supportsAllDrives=True
        ).execute()
        print(f"[Drive] Dùng folder: {meta.get('name')} ({meta.get('id')})")
        # cảnh báo: SA + My Drive (không có driveId) → không có quota
        creds = service._http.credentials
        using_sa = getattr(creds, "service_account_email", None) is not None
        if using_sa and not meta.get("driveId"):
            print("[Drive][WARN] Đang dùng Service Account vào folder My Drive → SA KHÔNG có quota để upload. "
                  "Hãy dùng OAuth (GDRIVE_OAUTH_TOKEN_JSON) hoặc chuyển sang Shared Drive.")
                  "Hãy dùng OAuth (GDRIVE_OAUTH_TOKEN_JSON) hoặc Shared Drive.")
        return meta["id"]
    except HttpError as e:
        print(f"[Drive] Không truy cập được Folder ID '{folder_id}': {e}")
@@ -276,7 +263,6 @@ def drive_upload_file(service, file_path: Path, folder_id: str):
        ).execute()
        return created["id"], "created"

# -------- yt-dlp opts --------
BASE_YDL_OPTS = {
    "format": "bestaudio[ext=m4a]/bestaudio/best",
    "merge_output_format": "m4a",
@@ -294,8 +280,11 @@ def drive_upload_file(service, file_path: Path, folder_id: str):
    "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"},
    "force_ipv4": True,
}
if FFMPEG_DIR:
    BASE_YDL_OPTS["ffmpeg_location"] = FFMPEG_DIR  # chỉ set khi có đủ ffmpeg & ffprobe cùng thư mục
if _resolve_ffmpeg_dir():
    # Gọi lại để đảm bảo đúng trạng thái cuối (đã in log ở trên); chỉ set khi đủ cặp ffmpeg+ffprobe
    FFMPEG_DIR2 = _resolve_ffmpeg_dir()
    if FFMPEG_DIR2:
        BASE_YDL_OPTS["ffmpeg_location"] = FFMPEG_DIR2

ROTATE_TRIGGERS = (
    "Sign in to confirm you’re not a bot",
@@ -328,10 +317,6 @@ def _ydl_opts_with_client(base_opts: dict, player_clients: list, cookiefile: Opt
    return opts

def try_download_with_cookies(url: str) -> Tuple[bool, Optional[str], Optional[Path]]:
    """
    Trả về (ok, err, file_path).
    Luôn thử đủ client trong CÙNG cookie set TRƯỚC khi xoay sang cookie khác (né SABR/403).
    """
    global last_good_cookie_idx
    order = list(range(len(COOKIE_FILES))) if COOKIE_FILES else [None]
    if COOKIE_FILES and last_good_cookie_idx < len(COOKIE_FILES):
@@ -362,12 +347,10 @@ def try_download_with_cookies(url: str) -> Tuple[bool, Optional[str], Optional[P
                return True, None, latest_file
            except Exception as e:
                last_err = str(e)
                # Dù 403/429/... vẫn thử client kế tiếp trong cùng cookie
                continue

    return False, (last_err or "Blocked/failed on all cookie sets/clients."), latest_file

# -------- Main --------
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
drive_service = init_drive_service()
resolved_folder_id = ensure_folder_by_id(drive_service, GDRIVE_FOLDER_ID) if drive_service else None
@@ -384,7 +367,7 @@ def try_download_with_cookies(url: str) -> Tuple[bool, Optional[str], Optional[P
        except Exception as e:
            print(f"[Drive] Smoke test lỗi: {e}")

for i, url in enumerate(run_list, 1):  # chỉ 1 link
for i, url in enumerate(run_list, 1):
    print(f"\n[{i}/{len(run_list)}] Download M4A: {url}")
    ok, err, fpath = try_download_with_cookies(url)
    if ok:
@@ -407,7 +390,6 @@ def try_download_with_cookies(url: str) -> Tuple[bool, Optional[str], Optional[P
            print(f"   Nghỉ {t}s...", end="\r"); time.sleep(1)
        print(" " * 24, end="\r")

# Đồng bộ dalay.txt (không bắt buộc)
if drive_service and resolved_folder_id and DALAY.exists():
    try:
        fid, action = drive_upload_file(drive_service, DALAY, resolved_folder_id)
