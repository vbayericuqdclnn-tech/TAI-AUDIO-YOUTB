name: YouTube Audio → Drive (10 links/run, ~2min cadence)

on:
  workflow_dispatch: {}
  # GitHub chỉ cho schedule tối thiểu 5 phút
  schedule:
    - cron: "*/5 * * * *"   # every 5 minutes (UTC)

permissions:
  contents: write
  actions: write   # cần để createWorkflowDispatch

concurrency:
  group: yt-audio-drive
  cancel-in-progress: false

jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 180
    env:
      # --- ĐÍCH ---
      GDRIVE_FOLDER_ID: "1V9qkTesXnogNiTPUvDZOwJPj4Op4lTHF"
      # Ưu tiên OAuth (tránh quota SA ở My Drive)
      GDRIVE_OAUTH_TOKEN_JSON: ${{ secrets.GDRIVE_OAUTH_TOKEN_JSON }}
      # Fallback SA (nếu không có OAuth)
      GDRIVE_SA_JSON: ${{ secrets.GDRIVE_SA_JSON }}

      # --- DL ---
      PO_TOKEN: ${{ secrets.PO_TOKEN }}
      SLEEP_SECONDS: "2"
      MAX_PER_RUN: "20"  # xử lý tối đa 20 link mỗi lần chạy

      # --- AUTO RE-DISPATCH ~2 PHÚT ---
      ENABLE_SELF_DISPATCH: "true"
      REDISPATCH_DELAY_SEC: "120"  # 2 phút

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python (cache pip)
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"
          cache-dependency-path: "requirements.txt"

      - name: Install Python deps
        run: |
          python -m pip install -U pip
          pip install -r requirements.txt

      - name: Install system ffmpeg (with ffprobe)
        run: |
          sudo apt-get update
          sudo apt-get install -y ffmpeg

      - name: Ensure data folder & files
        run: |
          mkdir -p data/audio
          [ -f data/links.txt ] || : > data/links.txt
          [ -f data/dalay.txt ] || : > data/dalay.txt

      - name: Debug links.txt (head)
        run: |
          echo "Branch: $(git rev-parse --abbrev-ref HEAD)"
          echo "links.txt size: $(wc -c < data/links.txt) bytes | lines: $(wc -l < data/links.txt)"
          sed -n '1,10p' data/links.txt | nl -ba || true
          echo "done lines: $(wc -l < data/dalay.txt || echo 0)"

      - name: Run downloader (tối đa ${{ env.MAX_PER_RUN }} link, upload thẳng Drive)
        env:
          PYTHONUNBUFFERED: "1"
          PYTHONUTF8: "1"
        run: |
          python -u scripts/yt_audio_to_drive.py

      - name: Commit updated dalay.txt (if changed)
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          if git diff --quiet --exit-code -- data/dalay.txt; then
            echo "No changes in dalay.txt"
          else
            git add data/dalay.txt
            git commit -m "chore: update dalay.txt (YouTube audio processed)"
            git push
          fi

      # Tự gọi lại workflow ~2 phút NẾU còn link chưa xử lý → chạy mãi đến khi hết
      - name: Self re-dispatch after delay (until done)
        if: ${{ env.ENABLE_SELF_DISPATCH == 'true' }}
        uses: actions/github-script@v7
        with:
          script: |
            const { execSync } = require('child_process');
            const delaySec = parseInt(process.env.REDISPATCH_DELAY_SEC || '120', 10);

            function pendingCount() {
              try {
                const cmd = `
                  bash -lc '
                    # Lọc dòng trống & comment, chuẩn hóa rồi so sánh
                    grep -Ev "^\\s*(#|$)" data/links.txt | sed "s/[[:space:]]\\+$//" | sort -u > /tmp/all || true
                    grep -Ev "^\\s*(#|$)" data/dalay.txt | sed "s/[[:space:]]\\+$//" | sort -u > /tmp/done || true
                    comm -23 /tmp/all /tmp/done | wc -l
                  '
                `;
                return parseInt(execSync(cmd, {stdio: ['ignore','pipe','pipe']}).toString().trim(), 10) || 0;
              } catch (e) { return 0; }
            }

            const remain = pendingCount();
            core.info(`Remaining links: ${remain}`);
            if (remain <= 0) {
              core.info('No remaining links. Skip redispatch.');
              return;
            }

            core.info(`Sleeping ${delaySec}s before redispatch...`);
            await new Promise(r => setTimeout(r, delaySec * 1000));

            core.info('Redispatching current workflow on same ref...');
            await github.rest.actions.createWorkflowDispatch({
              owner: context.repo.owner,
              repo: context.repo.repo,
              workflow_id: process.env.GITHUB_WORKFLOW,   // chính workflow hiện tại
              ref: context.ref,
              inputs: {}
            });
            core.info('Dispatched new run.')
