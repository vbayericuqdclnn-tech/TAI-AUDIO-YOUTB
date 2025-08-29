# -*- coding: utf-8 -*-
# YouTube audio -> M4A -> Google Drive
# Bản đã FIX: luôn ghi dalay.txt đúng chỗ, đúng format, không trùng.

import os, sys, re, json, time, shutil, tempfile, io
import fcntl
from pathlib import Path
from typing import Optional, Tuple, List

# -------------------- STDOUT unbuffered --------------------
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    os.environ["PYTHONUNBUFFERED"] = "1"

# -------------------- BASE PATH AN TOÀN --------------------
# Dùng CWD (GitHub Actions chạy ở root repo) để tránh "chệch" khi file nằm ngoài /scripts
REPO_ROOT = Path.cwd()
DATA_DIR  = REPO_ROOT / "data"
OUT_DIR   = DATA_DIR / "audio"
LINKS     = DATA_DIR / "links.txt"
DALAY     = DATA_DIR / "dalay.txt"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
if not LINKS.exists(): LINKS.write_text("", encoding="utf-8")
if not DALAY.exists(): DALAY.write_text("", encoding="utf-8")

# In debug đường dẫn tuyệt đối để bạn kiểm chứng đúng file.
print(f"[PATH] REPO_ROOT = {REPO_ROOT}")
print(f"[PATH] LINKS     = {LINKS.resolve()}")
print(f"[PATH] DALAY     = {DALAY.resolve()}")

SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "8"))
MAX_PER_RUN   = int(os.environ.get("MAX_PER_RUN", "100"))

# ------------------- FFMPEG LOCATE -------------------
def _resolve_ffmpeg_dir() -> Optional[str]:
    ffmpeg_bin  = shutil.which("ffmpeg")
    ffprobe_bin = shutil.which("ffprobe")
    if ffmpeg_bin and ffprobe_bin:
        p = Path(ffmpeg_bin).parent
        print(f"[ffmpeg] Dùng ffmpeg/ffprobe từ PATH: {p}")
        return str(p)
    print("[ffmpeg] Không tìm thấy ffmpeg/ffprobe trong PATH.")
    return None

FFMPEG_DIR = _resolve_ffmpeg_dir()

# ------------------- CANONICALIZE URL -------------------
YT_ID_RE = re.compile(
    r"""(?ix)
    (?:https?://)?(?:www\.)?
    (?:youtube\.com/(?:watch\?v=|shorts/|live/)|youtu\.be/)
    ([A-Za-z0-9_-]{6,})  # video id
    """
)

def canon_url(url: str) -> str:
    url = (url or "").strip()
    m = YT_ID_RE.search(url)
    if not m:  # không nhận ra -> trả nguyên (đã strip)
        return url
    vid = m.group(1)
    return f"https://www.youtube.com/watch?v={vid}"

# ------------------- IO AN TOÀN (LOCK + FSYNC) -------------------
def _ensure_file(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("", encoding="utf-8")

def _locked_append_line(path: Path, line: str):
    """Append với flock + fsync, có check trùng trong cùng process."""
    _ensure_file(path)
    line = line.rstrip("\n")
    with open(path, "a+", encoding="utf-8") as f:
        try: fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        except Exception: pass
        f.seek(0, io.SEEK_SET)
        exists = any(l.strip() == line for l in f.readlines())
        if not exists:
            f.seek(0, io.SEEK_END)
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())
            print(f"[DALAY] + {line} -> {path.resolve()}")
        else:
            print(f"[DALAY] = (đã có) {line}")
        try: fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception: pass

def read_lines_clean(p: Path) -> List[str]:
    if not p.exists(): return []
    lines = [ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines()]
    return [ln for ln in lines if ln and not ln.lstrip().startswith("#")]

def dedupe_dalay_against_links():
    """Chuẩn hoá, bỏ trùng, chỉ giữ những URL có trong links.txt và theo đúng thứ tự links.txt."""
    all_links = [canon_url(x) for x in read_lines_clean(LINKS)]
    done_set  = {canon_url(x) for x in read_lines_clean(DALAY)}
    filtered  = [u for u in all_links if u in done_set]
    tmp = DALAY.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for u in filtered:
            f.write(u + "\n")
    os.replace(tmp, DALAY)
    print(f"[DALAY] dedupe & align -> {DALAY.resolve()} (rows={len(filtered)})")

def log_done(original_url: str):
    """Ghi ngay khi *thành công*. Luôn canonicalize để match comm -23."""
    cu = canon_url(original_url)
    _locked_append_line(DALAY, cu)

# ------------------- yt-dlp & Drive -------------------
import yt_dlp
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

SCOPES = ["https://www.googleapis.com/auth/drive"]

def load_oauth_from_env() -> Optional[Credentials]:
    tok = os.environ.get("GDRIVE_OAUTH_TOKEN_JSON", "").strip()
    if not tok: return None
    try:
        info = json.loads(tok)
        return Credentials.from_authorized_user_info(info, SCOPES)
    except Exception as e:
        print(f"[Drive] OAuth token JSON không hợp lệ: {e}")
        return None

def load_sa_credentials() -> Optional[service_account.Credentials]:
    sa_json_text = os.environ.get("GDRIVE_SA_JSON", "").strip()
    try:
        if sa_json_text:
            info = json.loads(sa_json_text)
            if info.get("type") == "service_account":
                return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    except Exception as e:
        print(f"[Drive] SA lỗi: {e}")
    return None

def init_drive_service():
    creds = load_oauth_from_env()
    if creds:
        try:
            print("[Drive] Dùng OAuth từ secrets.")
            return build("drive", "v3", credentials=creds)
        except Exception as e:
            print(f"[Drive] Lỗi init OAuth: {e}")
    sa = load_sa_credentials()
    if sa:
        try:
            print("[Drive] Dùng Service Account.")
            return build("drive", "v3", credentials=sa)
        except Exception as e:
            print(f"[Drive] Lỗi init SA: {e}")
    print("[Drive] Không có OAuth/SA → bỏ qua upload Drive.")
    return None

def ensure_folder_by_id(service, folder_id: str) -> Optional[str]:
    if not service or not folder_id:
        print("[Drive] Thiếu service hoặc FolderID"); return None
    try:
        meta = service.files().get(fileId=folder_id, fields="id,name,driveId", supportsAllDrives=True).execute()
        print(f"[Drive] Folder: {meta.get('name')} ({meta.get('id')})")
        return meta["id"]
    except HttpError as e:
        print(f"[Drive] Không truy cập được Folder ID '{folder_id}': {e}")
        return None

def drive_upload_file(service, file_path: Path, folder_id: str):
    name = file_path.name
    q = "name = '{}' and '{}' in parents and trashed = false".format(name.replace("'", "\\'"), folder_id)
    res = service.files().list(
        q=q, pageSize=1, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True
    ).execute()
    media = MediaFileUpload(str(file_path), mimetype="audio/mp4", resumable=True)
    files = res.get("files", [])
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
    "quiet": False,
    "windowsfilenames": True,
    "nocheckcertificate": True,
    "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "m4a", "preferredquality": "0"}],
    "cachedir": str(REPO_ROOT / ".ydl_cache"),
    "retries": 3,
    "fragment_retries": 3,
    "http_headers": {"User-Agent": "Mozilla/5.0"},
    "force_ipv4": True,
}
ffdir = _resolve_ffmpeg_dir()
if ffdir: BASE_YDL_OPTS["ffmpeg_location"] = ffdir

def try_download(url: str) -> Tuple[bool, Optional[str], Optional[Path]]:
    """Tải 1 URL (không rotation cookie để giản lược; nếu cần, bạn giữ phần rotate cũ và chỉ ghép log_done như bên dưới)."""
    latest_file: Optional[Path] = None
    try:
        before = set(OUT_DIR.glob("*.m4a"))
        with yt_dlp.YoutubeDL(BASE_YDL_OPTS) as ydl:
            ydl.download([url])
        after = set(OUT_DIR.glob("*.m4a"))
        new_files = sorted(list(after - before), key=lambda p: p.stat().st_mtime, reverse=True)
        latest_file = new_files[0] if new_files else (sorted(list(after), key=lambda p: p.stat().st_mtime, reverse=True)[0] if after else None)
        return True, None, latest_file
    except Exception as e:
        return False, str(e), latest_file

GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
drive_service = init_drive_service()
resolved_folder_id = ensure_folder_by_id(drive_service, GDRIVE_FOLDER_ID) if drive_service else None

# ------------------- CHUẨN BỊ DANH SÁCH LINK -------------------
all_links_raw = read_lines_clean(LINKS)
all_links = [canon_url(x) for x in all_links_raw]
done_links = {canon_url(x) for x in read_lines_clean(DALAY)}
todo_links = [u for u in all_links if u not in done_links]

print(f"[COUNT] total={len(all_links)} | done={len(done_links)} | todo={len(todo_links)}")
run_list = todo_links[:MAX_PER_RUN]

success, failed, uploaded = [], [], []

if not run_list:
    print("[FLOW] Không có link mới để tải.")
else:
    for i, url in enumerate(run_list, 1):
        print(f"\n[{i}/{len(run_list)}] Download M4A: {url}")
        ok, err, fpath = try_download(url)
        if not ok:
            print(f" -> FAIL: {err}")
            failed.append((url, err))
        else:
            print(" -> OK: downloaded")
            logged = False
            # Nếu có Drive: chỉ log_done khi upload thành công
            if drive_service and resolved_folder_id and fpath and fpath.exists():
                try:
                    fid, action = drive_upload_file(drive_service, fpath, resolved_folder_id)
                    uploaded.append((fpath.name, action, fid))
                    print(f"    [Drive] {action}: {fpath.name} ({fid})")
                    log_done(url)      # <-- GHI Ở ĐÂY (sau khi UPLOAD THÀNH CÔNG)
                    logged = True
                except Exception as e:
                    print(f"    [Drive] Upload lỗi: {e}")
            # Không cấu hình Drive → log ngay sau khi download OK
            if not logged and (not drive_service or not resolved_folder_id):
                log_done(url)
                logged = True

            success.append(url)

        if i < len(run_list):
            for t in range(SLEEP_SECONDS, 0, -1):
                print(f"   Nghỉ {t}s...", end="\r"); time.sleep(1)
            print(" " * 24, end="\r")

# ------------------- DEDUPE & KẾT THÚC -------------------
dedupe_dalay_against_links()

# (Tuỳ chọn) upload dalay.txt lên Drive để kiểm chứng/backup
if drive_service and resolved_folder_id and DALAY.exists():
    try:
        fid, action = drive_upload_file(drive_service, DALAY, resolved_folder_id)
        print(f"[Drive] {action} dalay.txt ({fid})")
    except Exception as e:
        print(f"[Drive] Upload dalay.txt lỗi: {e}")

print("\n=== TỔNG KẾT ===")
print(f"OK: {len(success)} | FAIL: {len(failed)}")
print(f"M4A dir: {OUT_DIR.resolve()}")
if uploaded:
    print("Đã upload Drive:")
    for n, action, fid in uploaded:
        print(f" - {n} -> {action} ({fid})")
if failed:
    print("\nLỗi:")
    for u, e in failed:
        print(f"- {u}\n  Lý do: {e}\n")
