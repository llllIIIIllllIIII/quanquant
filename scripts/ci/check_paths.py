#!/usr/bin/env python3
"""check_paths.py — PR 變更檔案守門腳本（純標準庫，供 CI 與本機共用）

用途：
    QuanQuant repo 公開後，會有沒有軟體工程背景的協作者 fork 後開 PR 只想改
    前端（templates/static）。這支腳本比對 PR 的變更檔案清單，確認變更是否
    落在「前端協作者白名單」內；白名單外的變更預設會擋下（exit 1），除非 PR
    掛上 `backend-change` label（由維護者加註，代表已審過）；即使有 label，
    「永遠禁區」（CI/部署/測試/依賴鎖定等地基設施）仍然一律擋下，只能由
    維護者自己在 main 上修改。白名單內但需要人工特別留意的敏感前端檔案
    （會動到防呆機制/風控/認證的 JS 或 template）只會標成警告，不會擋 PR。

本機用法：
    uv run python scripts/ci/check_paths.py origin/main HEAD
    uv run python scripts/ci/check_paths.py origin/main HEAD        # 一般檢查
    PR_LABELS=backend-change uv run python scripts/ci/check_paths.py origin/main HEAD
                                                                     # 模擬已核准 backend-change label
    uv run python scripts/ci/check_paths.py --help

引數：
    base    diff 的基準 ref（例如 origin/main）
    head    diff 的目標 ref（例如 HEAD 或 PR 的 head sha）
    比對邏輯等同 `git diff --name-only <base>...<head>`（三點 diff，只看
    base 與 head 分岔點之後 head 端實際新增的變更，不受 base 之後新提交
    影響）。

環境變數：
    PR_AUTHOR / REPO_OWNER  PR 作者與 repo owner 帳號；相同即維護者模式（守門只提示不擋）。
                            本機模擬：PR_AUTHOR=me REPO_OWNER=me
    PR_LABELS   逗號分隔的 PR label 清單（CI 中由
                `${{ join(github.event.pull_request.labels.*.name, ',') }}` 傳入）。
                含 "backend-change" 時，白名單外的變更降級為警告（exit 0 可能），
                但永遠禁區仍然 exit 1。

輸出：
    以 GitHub Actions 的 ::error:: / ::warning:: annotation 格式印出，並在
    有 GITHUB_STEP_SUMMARY 環境變數時，把摘要 append 進去。
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import subprocess
import sys

# 白名單：允許前端協作者直接改的路徑（fnmatch 的 * 會吃掉包含 / 的任意長度字串，
# 所以 "dir/*" 已等同其他語言常見的 "dir/**"）。
WHITELIST_PATTERNS = [
    "src/quanquant/web/templates/*",
    "src/quanquant/web/static/*",
    "docs/*.md",
]

# 白名單內、但牽涉防呆/風控/認證，需要人工特別留意的敏感前端檔案（僅示警，不擋 PR）。
SENSITIVE_EXACT_FILES = {
    "src/quanquant/web/static/chart.js",
    "src/quanquant/web/static/chart-guards.js",
}
SENSITIVE_TEMPLATE_KEYWORDS = ["kill_switch", "cooldown", "cooling", "connection", "login", "token", "admin"]

# 永遠禁區：即使 PR 掛了 backend-change label 也一律擋下，只能維護者自己在 main 上改。
FOREVER_FORBIDDEN_PATTERNS = [
    ".github/*",
    "AGENTS.md",
    "CLAUDE.md",
    "CONTRIBUTING.md",
    "scripts/*",
    "pyproject.toml",
    "uv.lock",
    "tests/*",
    "docker-compose*",
    "Caddyfile*",
    "Dockerfile*",
]

BACKEND_CHANGE_LABEL = "backend-change"
MAINTAINER_CHANGE_LABEL = "maintainer-change"  # 維護者（或協同維護者）PR：守門只提示不擋



def matches_any(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


# -c core.quotePath=false：避免檔名含非 ASCII（例如中文檔名）時，git 把路徑輸出成
# 用引號包住的八進位跳脫序列（例如 "docs/\344\270\213..."），導致後續字串比對失敗。
_GIT_BASE_ARGS = ["git", "-c", "core.quotePath=false"]


def git_diff_names(base: str, head: str) -> list[str]:
    result = subprocess.run(
        [*_GIT_BASE_ARGS, "diff", "--name-only", f"{base}...{head}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def git_diff_text(base: str, head: str, path: str) -> str:
    result = subprocess.run(
        [*_GIT_BASE_ARGS, "diff", f"{base}...{head}", "--", path],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def has_hx_confirm_change(base: str, head: str, path: str) -> bool:
    diff_text = git_diff_text(base, head, path)
    for line in diff_text.splitlines():
        if line.startswith("+++") or line.startswith("---"):
            continue
        if line.startswith("+") or line.startswith("-"):
            if "hx-confirm" in line:
                return True
    return False


def classify(files: list[str], base: str, head: str):
    forever_hits: list[str] = []
    outside_whitelist: list[str] = []
    sensitive_hits: list[tuple[str, str]] = []

    for f in files:
        if matches_any(f, FOREVER_FORBIDDEN_PATTERNS):
            forever_hits.append(f)
            continue

        if not matches_any(f, WHITELIST_PATTERNS):
            outside_whitelist.append(f)
            continue

        # 在白名單內 -> 檢查是否為需要特別留意的敏感前端檔案
        if f in SENSITIVE_EXACT_FILES:
            sensitive_hits.append((f, "敏感前端核心檔案（圖表防呆/十字游標邏輯）"))
            continue

        if f.startswith("src/quanquant/web/templates/"):
            basename = os.path.basename(f)
            if any(keyword in basename for keyword in SENSITIVE_TEMPLATE_KEYWORDS):
                sensitive_hits.append((f, "檔名含敏感關鍵字（" + "/".join(SENSITIVE_TEMPLATE_KEYWORDS) + "）"))
            elif f.endswith(".html") and has_hx_confirm_change(base, head, f):
                sensitive_hits.append((f, "diff 中有 hx-confirm 屬性增減（可能改動確認防呆流程）"))

    return forever_hits, outside_whitelist, sensitive_hits


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="檢查 PR 變更檔案是否落在前端協作者白名單內，並標示敏感檔案。",
    )
    parser.add_argument("base", help="diff 基準 ref，例如 origin/main")
    parser.add_argument("head", help="diff 目標 ref，例如 HEAD 或 PR head sha")
    args = parser.parse_args(argv)

    files = git_diff_names(args.base, args.head)
    if not files:
        print("check_paths: 無變更檔案。")
        return 0

    forever_hits, outside_whitelist, sensitive_hits = classify(files, args.base, args.head)

    labels_raw = os.environ.get("PR_LABELS", "")
    labels = {label.strip() for label in labels_raw.split(",") if label.strip()}
    has_backend_label = BACKEND_CHANGE_LABEL in labels
    # 維護者模式：PR 作者就是 repo owner（branch protection 下 owner 也得走 PR），
    # 或維護者掛了 maintainer-change label（fork 貢獻者無權掛 label）。
    # 此模式下永遠禁區與白名單外都降為 warning，敏感檔照樣示警。
    pr_author = os.environ.get("PR_AUTHOR", "").strip().lower()
    repo_owner = os.environ.get("REPO_OWNER", "").strip().lower()
    is_maintainer = (bool(pr_author) and pr_author == repo_owner) or (MAINTAINER_CHANGE_LABEL in labels)

    exit_code = 0
    summary: list[str] = ["## check_paths 檔案路徑守門結果"]

    if sensitive_hits:
        summary.append("### 敏感前端檔案異動（在白名單內，僅示警，請人工複查）")
        for f, reason in sensitive_hits:
            print(f"::warning file={f}::敏感前端檔案異動：{f}（{reason}）")
            summary.append(f"- `{f}` — {reason}")

    if outside_whitelist:
        if has_backend_label or is_maintainer:
            summary.append(f"### 白名單外異動（已掛 `{BACKEND_CHANGE_LABEL}` label，降級為警告）")
            for f in outside_whitelist:
                print(f"::warning file={f}::白名單外變更（已核准 {BACKEND_CHANGE_LABEL} label）：{f}")
                summary.append(f"- `{f}`（已核准）")
        else:
            summary.append(
                f"### 白名單外異動（需維護者加 `{BACKEND_CHANGE_LABEL}` label 才能放行，或自行調整 PR）"
            )
            for f in outside_whitelist:
                print(f"::error file={f}::白名單外變更，需維護者加上 {BACKEND_CHANGE_LABEL} label 才可放行：{f}")
                summary.append(f"- `{f}`")
            exit_code = 1

    if forever_hits and is_maintainer:
        summary.append("### 永遠禁區異動（維護者 PR，降為提示；請自行複查）")
        for f in forever_hits:
            print(f"::warning file={f}::永遠禁區變更（維護者 PR，僅提示）：{f}")
            summary.append(f"- `{f}`（維護者）")
    elif forever_hits:
        summary.append("### 永遠禁區異動（label 也無法放行，只有維護者本人的 PR 可通過）")
        for f in forever_hits:
            print(f"::error file={f}::永遠禁區變更，貢獻者 PR（含 backend-change label）不可觸碰：{f}")
            summary.append(f"- `{f}`")
        exit_code = 1

    if exit_code == 0 and not sensitive_hits and not outside_whitelist and not forever_hits:
        summary.append("全部變更檔案皆在白名單內，無異常。")
        print("check_paths: 全部變更檔案皆在白名單內，通過。")
    elif exit_code == 0 and is_maintainer and (outside_whitelist or forever_hits):
        print("check_paths: 通過（維護者 PR，守門僅提示；請自行複查上方 warning）。")
    elif exit_code == 0 and outside_whitelist:
        print(f"check_paths: 通過（白名單外變更已由 {BACKEND_CHANGE_LABEL} label 核准降級，請維護者複查）。")
    elif exit_code == 0:
        print("check_paths: 通過（僅有敏感檔示警，未擋下 PR）。")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as fh:
            fh.write("\n".join(summary) + "\n")

    return exit_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
