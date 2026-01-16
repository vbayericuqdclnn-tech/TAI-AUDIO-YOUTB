# -*- coding: utf-8 -*-
# YouTube audio -> M4A -> Google Drive (FIXED VERSION)

import os, sys, re, json, time, shutil, tempfile
from pathlib import Path
from typing import Optional, Tuple, List

# --- CẤU HÌNH ---
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    os.environ["PYTHONUNBUFFERED"] = "1"

# Sửa lại đường dẫn để script chạy đúng trong GitHub Actions
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR  = REPO_ROOT / "data"
OUT_DIR   = DATA_DIR / "audio"
LINKS     = DATA_DIR / "links.txt"
DALAY     = DATA_DIR / "dalay.txt"
COOKIES_MULTI = DATA_DIR / "cookies_multi.txt"
PO_TOKEN_FILE = DATA_DIR / "po_token.txt"

# Tạo các thư mục cần thiết
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
if not LINKS.exists():
    LINKS.write_text("", encoding="utf-8")
if not DALAY.exists():
    DALAY.write_text("", encoding="utf-8")

SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "8"))
MAX_PER_RUN   = int(os.environ.get("MAX_PER_RUN", "40"))

# --- CÁC HÀM TIỆN ÍCH ---
def _resolve_ffmpeg_dir() -> Optional[str]:
    ffmpeg_bin  = shutil.which("ffmpeg")
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
        path   = c.get("path", "/")
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
        return []
    raw = cookies_multi_path.read_text(encoding="utf-8", errors="ignore")
    # Cho phép nhiều bộ cookie, ngăn cách bằng dòng ========
    parts = re.split(r"^\s*[=]{5,}\s*$", raw, flags=re.MULTILINE)
    cookie_files, idx = [], 0
    tmp_root = Path(tempfile.mkdtemp(prefix="cookies_sets_"))
    for part in parts:
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
            idx += 1
        else:
            print(f"[WARN] Bộ cookie #{idx} bỏ qua do thiếu khoá đăng nhập: {sorted(missing)}")
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
    Chỉ dùng credentials từ biến môi trường, không mở trình duyệt/flow local.
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
    name  = file_path.name
    media = MediaFileUpload(str(file_path), mimetype="audio/mp4", resumable=True)
    body  = {"name": name, "parents": [folder_id]}
    created = service.files().create(
        body=body,
        media_body=media,
        fields="id",
        supportsAllDrives=True
    ).execute()
    return created["id"], "created"

# --- LOGIC YT-DLP ---
BASE_YDL_OPTS = {
    "format": "bestaudio[ext=m4a]/bestaudio/best",
    "merge_output_format": "m4a",
    "outtmpl": str(OUT_DIR / "%(title)s.%(ext)s"),
    "noplaylist": True,
    "quiet": False,
    "nocheckcertificate": True,
    "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "m4a"}],
    "retries": 5,
    "fragment_retries": 5,
    "force_ipv4": True,
    # Né SABR tối đa
    "hls_use_mpegts": True,
    "concurrent_fragment_downloads": 8,
    "format_sort": ["proto:m3u8", "res"],
    "format_sort_force": False,
    # FIX YouTube JS challenge (EJS): cho phép yt-dlp tự tải solver script từ GitHub
    # (cần cài JS runtime như Deno trong workflow)
    "remote_components": {"ejs:github"},
}
if FFMPEG_DIR:
    BASE_YDL_OPTS["ffmpeg_location"] = FFMPEG_DIR

last_good_cookie_idx = 0  # nhớ bộ cookie nào gần nhất đã thành công

def _ydl_opts_with_client(base_opts: dict, player_clients: list, cookiefile: Optional[str], po_tok: str):
    opts = dict(base_opts)
    ex_args = {"youtube": {"player_client": player_clients}}
    if po_tok and any(pc.startswith("web") for pc in player_clients):
        ex_args["youtube"]["po_token"] = [f"web+{po_tok}"]
    opts["extractor_args"] = ex_args
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
    - Ưu tiên bộ nào vừa thành công gần nhất (last_good_cookie_idx)
    - Cuối cùng fallback 1 lần: không dùng cookie (None)
    """
    global last_good_cookie_idx
    COOKIE_FILES = prepare_cookie_files(COOKIES_MULTI)

    if COOKIE_FILES:
        base_order = list(range(len(COOKIE_FILES)))
        if last_good_cookie_idx < len(COOKIE_FILES):
            base_order = list(range(last_good_cookie_idx, len(COOKIE_FILES))) + list(range(0, last_good_cookie_idx))
        order = base_order + [None]   # thêm fallback không cookie
    else:
        order = [None]

    last_err = "Không thể tải về với tất cả các tùy chọn."

    for ck_idx in order:
        cookiefile = COOKIE_FILES[ck_idx] if ck_idx is not None else None
        if cookiefile:
            plans = [["web_safari"], ["web_embedded"], ["web"], ["android"]]
            print(f"   -> Thử cookie set #{ck_idx}")
        else:
            # Không cookie: ưu tiên android để dễ ra HLS
            plans = [["android"], ["web_safari"], ["web_embedded"], ["web"]]

        for pcs in plans:
            try:
                ydl_opts = _ydl_opts_with_client(BASE_YDL_OPTS, pcs, cookiefile, po_token)
                before = _list_audio_files()

                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info_dict = ydl.extract_info(url, download=False)
                    filename = Path(ydl.prepare_filename(info_dict))
                    # Nếu file (bất kỳ đuôi nào) đã tồn tại, coi như thành công và bỏ qua
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
                else:
                    # Có thể đã tồn tại từ trước
                    return True, "Không có file mới được tạo (có thể đã tồn tại)", None

            except Exception as e:
                last_err = str(e)
                continue

    return False, last_err, None

# --- CHUẨN BỊ CHẠY ---
all_links  = read_lines_clean(LINKS)
done_links = set(read_lines_clean(DALAY))
run_list   = [url for url in all_links if url not in done_links][:MAX_PER_RUN]
print(f"Tổng: {len(all_links)} | Đã làm: {len(done_links)} | Sẽ xử lý trong lần chạy này: {len(run_list)}")

po_token = (
    os.environ.get("PO_TOKEN")
    or (PO_TOKEN_FILE.read_text(encoding="utf-8").strip() if PO_TOKEN_FILE.exists() else "")
).strip()

GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
drive_service     = init_drive_service()
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
