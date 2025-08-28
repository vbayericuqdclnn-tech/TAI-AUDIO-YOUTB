# -*- coding: utf-8 -*-
# YouTube audio -> M4A -> Google Drive
# - Mỗi lần chạy: xử lý N link (mặc định 10, qua env MAX_PER_RUN)
# - Cookie rotation + player-client rotation; PO_TOKEN tùy chọn
# - Upload Drive: ƯU TIÊN OAuth (GDRIVE_OAUTH_TOKEN_JSON), fallback SA (GDRIVE_SA_JSON)
# - Full Drive scope; fix ffmpeg/ffprobe; unbuffered logs

import os, sys, re, json, time, shutil, tempfile
from pathlib import Path
from typing import Optional, Tuple, List

try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    os.environ["PYTHONUNBUFFERED"] = "1"

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR  = REPO_ROOT / "data"
OUT_DIR   = DATA_DIR / "audio"
LINKS     = DATA_DIR / "links.txt"
DALAY     = DATA_DIR / "dalay.txt"
COOKIES_MULTI = DATA_DIR / "cookies_multi.txt"
PO_TOKEN_FILE = DATA_DIR / "po_token.txt"
TOKEN_STORE   = DATA_DIR / "drive_token.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
if not LINKS.exists(): LINKS.write_text("", encoding="utf-8")
if not DALAY.exists(): DALAY.write_text("", encoding="utf-8")

SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "8"))
SMOKE_TEST = os.environ.get("SMOKE_TEST", "0").strip() == "1"
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "20"))  # <<< chạy 20 link/lần

def _resolve_ffmpeg_dir() -> Optional[str]:
    ffmpeg_bin  = shutil.which("ffmpeg")
    ffprobe_bin = shutil.which("ffprobe")
    if ffmpeg_bin and ffprobe_bin:
        p1, p2 = Path(ffmpeg_bin).parent, Path(ffprobe_bin).parent
        if p1 == p2:
            print(f"[ffmpeg] Dùng system ffmpeg/ffprobe: {p1}")
            return str(p1)
        print(f"[ffmpeg] ffmpeg tại {p1}, ffprobe tại {p2} -> dùng PATH, không set ffmpeg_location")
        return None
    try:
        import imageio_ffmpeg  # noqa
        alt = Path(__import__("imageio_ffmpeg").get_ffmpeg_exe()).parent  # type: ignore
        print(f"[ffmpeg] Tìm thấy ffmpeg (imageio) tại: {alt} (không có ffprobe) -> dùng PATH")
    except Exception:
        print("[ffmpeg] Không thấy ffmpeg/ffprobe trong PATH.")
    return None

FFMPEG_DIR = _resolve_ffmpeg_dir()

import yt_dlp

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]

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

# --- chạy 10 link / lần (hoặc MAX_PER_RUN) ---
run_list = new_links[:MAX_PER_RUN]

po_token = (os.environ.get("PO_TOKEN") or (PO_TOKEN_FILE.read_text(encoding="utf-8").strip() if PO_TOKEN_FILE.exists() else "")).strip()

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
    creds = load_oauth_from_env()
    if creds:
        try:
            print("[Drive] Dùng OAuth token từ GDRIVE_OAUTH_TOKEN_JSON.")
            return build("drive", "v3", credentials=creds)
        except Exception as e:
            print(f"[Drive] Không khởi tạo được Drive service (OAuth): {e}")

    sa = load_sa_credentials()
    if sa:
        try:
            sa_email = getattr(sa, "service_account_email", None)
            if sa_email:
                print(f"[Drive] Service Account email: {sa_email}")
        except Exception:
            pass
        print("[Drive] Dùng Service Account.")
        try:
            return build("drive", "v3", credentials=sa)
        except Exception as e:
            print(f"[Drive] Không khởi tạo được Drive service (SA): {e}")
            return None

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
                    print("[Drive] Không có OAuth token, không có SA, cũng không có client_secret.json → bỏ qua upload Drive.")
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

def ensure_folder_by_id(service, folder_id: str) -> Optional[str]:
    if not service or not folder_id:
        print("[Drive] Thiếu service hoặc Folder ID."); return None
    try:
        meta = service.files().get(
            fileId=folder_id, fields="id,name,driveId", supportsAllDrives=True
        ).execute()
        print(f"[Drive] Dùng folder: {meta.get('name')} ({meta.get('id')})")
        creds = service._http.credentials
        using_sa = getattr(creds, "service_account_email", None) is not None
        if using_sa and not meta.get("driveId"):
            print("[Drive][WARN] Đang dùng Service Account vào folder My Drive → SA KHÔNG có quota để upload. "
                  "Hãy dùng OAuth (GDRIVE_OAUTH_TOKEN_JSON) hoặc Shared Drive.")
        return meta["id"]
    except HttpError as e:
        print(f"[Drive] Không truy cập được Folder ID '{folder_id}': {e}")
        return None

def _escape_drive_literal(s: str) -> str:
    return s.replace("'", "\\'")

def drive_upload_file(service, file_path: Path, folder_id: str):
    name = file_path.name
    esc_name = _escape_drive_literal(name)
    q = "name = '{}' and '{}' in parents and trashed = false".format(esc_name, folder_id)
    res = service.files().list(
        q=q, pageSize=1, fields="files(id, name, parents, driveId)",
        supportsAllDrives=True, includeItemsFromAllDrives=True
    ).execute()
    files = res.get("files", [])
    media = MediaFileUpload(str(file_path), mimetype="audio/mp4", resumable=True)
    if files:
        file_id = files[0]["id"]
        upd = service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
        return upd["id"], "updated"
    else:
        body = {"name": name, "parents": [folder_id]}
        created = service.files().create(
            body=body, media_body=media, fields="id", supportsAllDrives=True
        ).execute()
        return created["id"], "created"

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
}
if _resolve_ffmpeg_dir():
    # Gọi lại để đảm bảo đúng trạng thái cuối (đã in log ở trên); chỉ set khi đủ cặp ffmpeg+ffprobe
    FFMPEG_DIR2 = _resolve_ffmpeg_dir()
    if FFMPEG_DIR2:
        BASE_YDL_OPTS["ffmpeg_location"] = FFMPEG_DIR2

ROTATE_TRIGGERS = (
    "Sign in to confirm you’re not a bot",
    "Sign in to confirm you're not a bot",
    "HTTP Error 429",
    "HTTP Error 403",
    "Forbidden",
    "410: Gone",
    "HTTP Error 410",
    "This video is private",
    "Private video",
    "not available in your country",
    "proxy",
)
RETRY_TRIGGERS_IMAGES = (
    "Only images are available for download",
    "Requested format is not available",
)

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
    last_err = None

    for ck_idx in order:
        cookiefile = COOKIE_FILES[ck_idx] if ck_idx is not None else None
        if cookiefile:
            print(f"   -> thử cookie set #{ck_idx}")
            plans = [["web"], ["web_embedded"], ["android"]]
        else:
            plans = [["android"], ["web"], ["web_embedded"]]

        for pcs in plans:
            try:
                ydl_opts = _ydl_opts_with_client(BASE_YDL_OPTS, pcs, cookiefile, po_token)
                before = set(OUT_DIR.glob("*.m4a"))
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])
                after = set(OUT_DIR.glob("*.m4a"))
                new_files = sorted(list(after - before), key=lambda p: p.stat().st_mtime, reverse=True)
                latest_file = new_files[0] if new_files else (sorted(list(after), key=lambda p: p.stat().st_mtime, reverse=True)[0] if after else None)
                if ck_idx is not None:
                    last_good_cookie_idx = ck_idx
                return True, None, latest_file
            except Exception as e:
                last_err = str(e)
                continue

    return False, (last_err or "Blocked/failed on all cookie sets/clients."), latest_file

GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
drive_service = init_drive_service()
resolved_folder_id = ensure_folder_by_id(drive_service, GDRIVE_FOLDER_ID) if drive_service else None

success, failed, uploaded = [], [], []
if not run_list:
    print("Không có link mới để tải.")
    if drive_service and resolved_folder_id and SMOKE_TEST:
        testf = OUT_DIR / "SMOKE_TEST.txt"
        testf.write_text("ok", encoding="utf-8")
        try:
            fid, action = drive_upload_file(drive_service, testf, resolved_folder_id)
            print(f"[Drive] {action} SMOKE_TEST.txt ({fid})")
        except Exception as e:
            print(f"[Drive] Smoke test lỗi: {e}")

for i, url in enumerate(run_list, 1):
    print(f"\n[{i}/{len(run_list)}] Download M4A: {url}")
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

    if i < len(run_list):
        for t in range(SLEEP_SECONDS, 0, -1):
            print(f"   Nghỉ {t}s...", end="\r"); time.sleep(1)
        print(" " * 24, end="\r")

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
