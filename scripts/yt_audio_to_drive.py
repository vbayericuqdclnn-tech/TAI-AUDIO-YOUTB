# -*- coding: utf-8 -*-
import os, sys, re, json, time, shutil, tempfile
from pathlib import Path
from typing import Optional, Tuple, List

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR  = REPO_ROOT / "data"
OUT_DIR   = DATA_DIR / "audio"
LINKS     = DATA_DIR / "links.txt"
DALAY     = DATA_DIR / "dalay.txt"
COOKIES_MULTI = DATA_DIR / "cookies_multi.txt"
PO_TOKEN_FILE = DATA_DIR / "po_token.txt"
TOKEN_STORE   = DATA_DIR / "drive_token.json"  # dùng cho OAuth khi chạy local

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
if not LINKS.exists(): LINKS.write_text("", encoding="utf-8")
if not DALAY.exists(): DALAY.write_text("", encoding="utf-8")

SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "8"))

import yt_dlp

# ffmpeg portable trước, hệ thống sau
FFMPEG_DIR = None
try:
    import imageio_ffmpeg
    FFMPEG_DIR = str(Path(imageio_ffmpeg.get_ffmpeg_exe()).parent)
    print(f"[ffmpeg] Dùng ffmpeg portable: {FFMPEG_DIR}")
except Exception:
    bin_path = shutil.which("ffmpeg")
    if bin_path:
        FFMPEG_DIR = str(Path(bin_path).parent)
        print(f"[ffmpeg] Dùng ffmpeg hệ thống: {FFMPEG_DIR}")
    else:
        print("[ERROR] Không tìm thấy ffmpeg (thêm imageio-ffmpeg vào requirements.txt).")
        sys.exit(1)

# ----------------- Google Drive -----------------
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def read_lines_clean(p: Path) -> List[str]:
    if not p.exists(): return []
    lines = [ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines()]
    return [ln for ln in lines if ln and not ln.startswith("#")]

def _json_cookie_to_netscape_lines(js_text: str):
    try:
        data = json.loads(js_text)
        if not isinstance(data, list): return None
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
        if not domain or not name: continue
        out.append("\t".join([domain, include_sub, path, secure, expires, name, value]))
    return out

def _looks_like_netscape(txt: str) -> bool:
    for ln in txt.splitlines():
        if ln.startswith("#") or not ln.strip(): continue
        parts = ln.split("\t")
        if len(parts) == 7:
            try: int(parts[4]); return True
            except Exception: pass
    return False

def validate_cookie_file(path: Path):
    txt = path.read_text(encoding="utf-8", errors="ignore")
    names = set()
    for ln in txt.splitlines():
        if ln.startswith("#") or not ln.strip(): continue
        parts = ln.split("\t")
        if len(parts) == 7: names.add(parts[5])
    needed = {"SAPISID", "__Secure-3PSID", "__Secure-3PAPISID"}
    has_any = bool(needed & names) or ("SID" in names and "HSID" in names)
    missing = set() if has_any else needed
    return has_any, missing

def prepare_cookie_files(cookies_multi_path: Path) -> List[str]:
    if not cookies_multi_path.exists(): return []
    raw = cookies_multi_path.read_text(encoding="utf-8", errors="ignore")
    parts = re.split(r"^\s*[=]{5,}\s*$", raw, flags=re.MULTILINE)
    cookie_files, idx = [], 0
    tmp_root = Path(tempfile.mkdtemp(prefix="cookies_sets_"))
    for part in parts:
        content = part.strip()
        if not content: continue
        if not _looks_like_netscape(content):
            lines = _json_cookie_to_netscape_lines(content)
            if lines: content = "\n".join(lines)
        f = tmp_root / f"ck_{idx}.txt"
        f.write_text(content + ("\n" if not content.endswith("\n") else ""), encoding="utf-8")
        has_lines = any((ln.strip() and not ln.strip().startswith("#")) for ln in content.splitlines())
        ok, missing = validate_cookie_file(f)
        if has_lines and ok:
            cookie_files.append(str(f)); idx += 1
        else:
            print(f"[WARN] Bộ cookie #{idx} bỏ qua do thiếu khoá đăng nhập: {sorted(missing)}")
    return cookie_files

COOKIE_FILES = prepare_cookie_files(COOKIES_MULTI)
print(f"Cookies sets hợp lệ: {len(COOKIE_FILES)}" if COOKIE_FILES else "Không dùng cookies hoặc tất cả set không hợp lệ.")

all_links  = read_lines_clean(LINKS)
done_links = set(read_lines_clean(DALAY))
seen, new_links = set(), []
for url in all_links:
    if url in done_links or url in seen: continue
    seen.add(url); new_links.append(url)
print(f"Tổng: {len(all_links)} | Đã làm: {len(done_links)} | Mới sẽ xử lý: {len(new_links)}")

po_token = (os.environ.get("PO_TOKEN") or (PO_TOKEN_FILE.read_text(encoding="utf-8").strip() if PO_TOKEN_FILE.exists() else "")).strip()

# ---- Drive auth ----
def load_sa_credentials() -> Optional[service_account.Credentials]:
    sa_json_text = os.environ.get("GDRIVE_SA_JSON", "").strip()
    sa_file = os.environ.get("GDRIVE_SA_FILE", "").strip()
    try:
        if sa_json_text:
            info = json.loads(sa_json_text)
            if info.get("type") == "service_account":
                return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        if sa_file and Path(sa_file).exists():
            info = json.loads(Path(sa_file).read_text(encoding="utf-8"))
            if info.get("type") == "service_account":
                return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    except Exception as e:
        print(f"[Drive] Service Account lỗi: {e}")
    return None

def init_drive_service():
    creds = load_sa_credentials()
    if creds:
        # In email SA để bạn share folder đúng
        try:
            sa_email = getattr(creds, "service_account_email", None)
            if sa_email: print(f"[Drive] Service Account email: {sa_email}")
        except Exception:
            pass
        print("[Drive] Dùng Service Account.")
        try:
            return build("drive", "v3", credentials=creds)
        except Exception as e:
            print(f"[Drive] Không khởi tạo được Drive service (SA): {e}")
            return None
    # OAuth chỉ cho local (Actions không tương tác)
    try:
        creds = None
        if TOKEN_STORE.exists():
            creds = Credentials.from_authorized_user_file(str(TOKEN_STORE), SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
            else:
                maybe = list(REPO_ROOT.glob("client_secret*.json"))
                if not maybe:
                    print("[Drive] Không có SA cũng không có client_secret.json → bỏ qua upload Drive.")
                    return None
                flow = InstalledAppFlow.from_client_secrets_file(str(maybe[0]), SCOPES)
                print("[Drive] OAuth local: mở device flow...")
                creds = flow.run_console()
            TOKEN_STORE.write_text(creds.to_json(), encoding="utf-8")
        print("[Drive] Dùng OAuth Installed App (local).")
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        print(f"[Drive] OAuth lỗi: {e}")
        return None

# NEW: hỗ trợ My Drive & Shared Drive, tìm/tạo folder theo ID/Name
def ensure_drive_folder(service, folder_id: str, folder_name: str, drive_id: str) -> Optional[str]:
    if not service:
        print("[Drive] Chưa có service → bỏ qua.")
        return None

    # 1) Nếu có ID: thử lấy (hỗ trợ Shared Drives)
    if folder_id:
        try:
            meta = service.files().get(
                fileId=folder_id, fields="id,name,parents,driveId",
                supportsAllDrives=True
            ).execute()
            print(f"[Drive] Dùng folder: {meta.get('name')} ({meta.get('id')})")
            return meta["id"]
        except HttpError as e:
            print(f"[Drive] Không truy cập được Folder ID '{folder_id}': {e}")

    # 2) Nếu có tên: tìm theo tên
    if folder_name:
        q = "mimeType='application/vnd.google-apps.folder' and name='{}' and trashed=false".format(
            folder_name.replace("'", "\\'")
        )
        params = {
            "q": q,
            "pageSize": 10,
            "fields": "files(id,name,driveId,parents)",
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }
        if drive_id:
            params["corpora"] = "drive"
            params["driveId"] = drive_id
        else:
            params["corpora"] = "user"
        res = service.files().list(**params).execute()
        files = res.get("files", [])
        if files:
            fid = files[0]["id"]
            print(f"[Drive] Tìm thấy folder theo tên: {folder_name} ({fid})")
            return fid

        # 3) Không có → tạo mới
        body = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [drive_id if drive_id else "root"]
        }
        created = service.files().create(
            body=body, fields="id,name", supportsAllDrives=True
        ).execute()
        print(f"[Drive] Đã tạo folder: {created.get('name')} ({created.get('id')})")
        return created["id"]

    print("[Drive] Thiếu cả GDRIVE_FOLDER_ID lẫn GDRIVE_FOLDER_NAME.")
    return None

def _escape_drive_literal(s: str) -> str:
    return s.replace("'", "\\'")

def drive_upload_file(service, file_path: Path, folder_id: str):
    name = file_path.name
    esc_name = _escape_drive_literal(name)

    # supportsAllDrives cho Shared Drives
    q = "name = '{}' and '{}' in parents and trashed = false".format(esc_name, folder_id)
    res = service.files().list(
        q=q, pageSize=1, fields="files(id, name, parents, driveId)",
        supportsAllDrives=True, includeItemsFromAllDrives=True
    ).execute()
    files = res.get("files", [])

    media = MediaFileUpload(str(file_path), mimetype="audio/mp4", resumable=True)
    if files:
        file_id = files[0]["id"]
        upd = service.files().update(
            fileId=file_id, media_body=media, supportsAllDrives=True
        ).execute()
        return upd["id"], "updated"
    else:
        body = {"name": name, "parents": [folder_id]}
        created = service.files().create(
            body=body, media_body=media, fields="id", supportsAllDrives=True
        ).execute()
        return created["id"], "created"

# ==== yt-dlp config ====
BASE_YDL_OPTS = {
    "format": "bestaudio[ext=m4a]/bestaudio/best",
    "merge_output_format": "m4a",
    "outtmpl": str(OUT_DIR / "%(title)s.%(ext)s"),
    "noplaylist": True,
    "consoletitle": False,
    "quiet": False,
    "restrictfilenames": False,
    "windowsfilenames": True,
    "nocheckcertificate": True,
    "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "m4a", "preferredquality": "0"}],
    "cachedir": str(REPO_ROOT / ".ydl_cache"),
    "retries": 3,
    "fragment_retries": 3,
    "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"},
    "force_ipv4": True,
    "ffmpeg_location": FFMPEG_DIR,
}
ROTATE_TRIGGERS = (
    "Sign in to confirm you’re not a bot", "Sign in to confirm you're not a bot",
    "HTTP Error 429", "HTTP Error 403", "Forbidden", "410: Gone", "HTTP Error 410",
    "This video is private", "Private video", "not available in your country", "proxy",
)
RETRY_TRIGGERS_IMAGES = ("Only images are available for download", "Requested format is not available")
last_good_cookie_idx = 0

def _ydl_opts_with_client(base_opts: dict, player_clients: list, cookiefile: Optional[str], po_tok: str):
    opts = dict(base_opts)
    ex_args = {"youtube": {"player_client": player_clients}}
    if po_tok and any(pc.startswith("web") for pc in player_clients):
        ex_args["youtube"]["po_token"] = [f"web+{po_tok}"]
    opts["extractor_args"] = ex_args
    if cookiefile: opts["cookiefile"] = cookiefile
    else: opts.pop("cookiefile", None)
    return opts

def try_download_with_cookies(url: str) -> Tuple[bool, Optional[str], Optional[Path]]:
    global last_good_cookie_idx
    order = list(range(len(COOKIE_FILES))) if COOKIE_FILES else [None]
    if COOKIE_FILES and last_good_cookie_idx < len(COOKIE_FILES):
        order = list(range(last_good_cookie_idx, len(COOKIE_FILES))) + list(range(0, last_good_cookie_idx))
    latest_file: Optional[Path] = None
    for ck_idx in order:
        cookiefile = COOKIE_FILES[ck_idx] if ck_idx is not None else None
        if cookiefile: print(f"   -> thử cookie set #{ck_idx}")
        plans = [["android"], ["web"], ["web_embedded"]] if not cookiefile else [["web"], ["web_embedded"], ["android"]]
        last_err = None
        for pcs in plans:
            try:
                ydl_opts = _ydl_opts_with_client(BASE_YDL_OPTS, pcs, cookiefile, po_token)
                before = set(p for p in OUT_DIR.glob("*.m4a"))
                with yt_dlp.YoutubeDL(ydl_opts) as ydl: ydl.download([url])
                after = set(p for p in OUT_DIR.glob("*.m4a"))
                new_files = sorted(list(after - before), key=lambda p: p.stat().st_mtime, reverse=True)
                latest_file = new_files[0] if new_files else (sorted(list(after), key=lambda p: p.stat().st_mtime, reverse=True)[0] if after else None)
                if ck_idx is not None: last_good_cookie_idx = ck_idx
                return True, None, latest_file
            except Exception as e:
                msg = str(e); last_err = msg
                if any(t.lower() in msg.lower() for t in RETRY_TRIGGERS_IMAGES): continue
                if cookiefile:
                    if any(trig.lower() in msg.lower() for trig in ROTATE_TRIGGERS): break
                else: continue
    return False, (last_err or "Blocked/failed on all cookie sets/clients."), latest_file

# ---- Drive setup & main loop ----
GDRIVE_FOLDER_ID   = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
GDRIVE_FOLDER_NAME = os.environ.get("GDRIVE_FOLDER_NAME", "").strip()  # tuỳ chọn
GDRIVE_DRIVE_ID    = os.environ.get("GDRIVE_DRIVE_ID", "").strip()     # tuỳ chọn (Shared Drive ID)

drive_service = init_drive_service()
resolved_folder_id = None
if drive_service:
    resolved_folder_id = ensure_drive_folder(
        drive_service, GDRIVE_FOLDER_ID, GDRIVE_FOLDER_NAME, GDRIVE_DRIVE_ID
    )

success, failed, uploaded = [], [], []
if not new_links:
    print("Không có link mới để tải.")

for i, url in enumerate(new_links, 1):
    print(f"\n[{i}/{len(new_links)}] Download M4A: {url}")
    ok, err, fpath = try_download_with_cookies(url)
    if ok:
        DALAY.open("a", encoding="utf-8").write(url + "\n")
        success.append(url)
        print(" -> OK")
        if drive_service and resolved_folder_id and fpath and fpath.exists():
            try:
                fid, action = drive_upload_file(drive_service, fpath, resolved_folder_id)
                uploaded.append((fpath.name, action, fid))
                print(f"    [Drive] {action}: {fpath.name} ({fid})")
            except Exception as ue:
                print(f"    [Drive] Upload lỗi: {ue}")
    else:
        failed.append((url, err))
        print(f" -> FAIL: {err}")

    if i < len(new_links):
        for t in range(SLEEP_SECONDS, 0, -1):
            print(f"   Nghỉ {t}s...", end="\r"); time.sleep(1)
        print(" " * 24, end="\r")

# Đồng bộ dalay.txt (không bắt buộc)
if drive_service and resolved_folder_id and DALAY.exists():
    try:
        fid, action = drive_upload_file(drive_service, DALAY, resolved_folder_id)
        print(f"[Drive] {action} dalay.txt ({fid})")
    except Exception as e:
        print(f"[Drive] Upload dalay.txt lỗi: {e}")

print("\n=== TỔNG KẾT ===")
print(f"OK: {len(success)} | FAIL: {len(failed)}")
print(f"Đã lưu file M4A vào: {OUT_DIR}")
if uploaded:
    print("Đã upload Drive:")
    for n, action, fid in uploaded:
        print(f" - {n} -> {action} ({fid})")
if failed:
    print("\nDanh sách lỗi:")
    for u, e in failed:
        print(f"- {u}\n  Lý do: {e}\n")
