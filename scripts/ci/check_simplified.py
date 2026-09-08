#!/usr/bin/env python3
"""check_simplified.py — 掃描檔案中混入的簡體中文字（純標準庫）

用途：
    QuanQuant 全站規定使用繁體中文（台灣用語），但公開 repo 後協作者可能用
    習慣簡體輸入法或 AI 產生內容夾帶簡體字。這支腳本用一份「無歧義簡體字」
    清單（清單裡每個字都是繁體中文正文絕不會出現的字形，例如部首簡化字
    〔言字旁/金字旁/糸字旁/食字旁/門字旁/貝字旁/見字旁/馬字旁/鳥字旁〕，
    以及整字簡化字；刻意排除兩岸共用、會有歧義的字）逐字掃描，命中就視為
    疑似簡體字混入。

本機用法：
    uv run python scripts/ci/check_simplified.py
        不帶參數時，掃描 src/quanquant/web/ 與 docs/ 底下所有
        .html/.js/.css/.py/.md 檔案。

    uv run python scripts/ci/check_simplified.py path/a.html path/b.js
        帶參數時，只掃描指定的檔案（CI 的 pr-guard job 會傳入 PR diff 中
        新增/修改的檔案清單）。

    uv run python scripts/ci/check_simplified.py --help
        顯示本說明。

輸出格式：
    命中時每一行印出：
        路徑:行號: 字 (該行前 60 字)
    有任何命中就 exit 1；完全沒有命中則印出統計訊息並 exit 0。

注意：
    這是「找疑似」的粗篩工具，不是語言學等級的簡繁判定；清單刻意保守，
    只收錄真正無歧義（繁體絕不使用）的字形，寧可漏抓也不要誤傷正常繁體
    用字。若要擴充清單，新增前務必用本腳本對現有 main 全量掃過一次確認
    沒有誤報。
"""

from __future__ import annotations

import sys
from pathlib import Path

# 「無歧義簡體字」清單：僅收錄繁體中文正文絕不會出現的字形，依簡化方式分組。
# 已刻意排除兩岸共用、會有歧義的字（該類字繁體語境也用得到，收進來會誤傷）。
SIMPLIFIED_ONLY_CHARS = set(
    # 言部簡化（繁體一律用完整的言部件，不會簡寫成單人旁加點的簡化形）
    "计订认讨让训议讲许论设访证评识诉诊词试诗话详误语说请诸读课谁谈谋谐谢讯诚"
    # 金部簡化
    "针钓钙钝钞钟钢钥钦钩钮钱钳钻铁铃铅铜银锁"
    # 糸部簡化
    "红约级纯纲纳纸线练组细织终结给络统继"
    # 食部簡化
    "饥饨饪饭饮饱饲饺饼馆"
    # 門部簡化
    "们闪闭问闯间闲闷闹闻"
    # 貝部簡化
    "贝负贡财责贤败账货质购贯贵贷贸费贺资"
    # 見部簡化
    "观规觅览觉觊觎"
    # 馬部簡化
    "马驭驰驱驳驴骂验骑骗"
    # 鳥部簡化
    "鸟鸡鸣鸦鸭鸽鹰"
    # 整字簡化（逐字確認過繁體不會另作他用）
    "国学长现实头关产电车农医药华会体儿写应这进远运还"
    "审导对层属币师帮广库历压县参数汉测润涨渐"
    "龙齐归书权转软轻较输载环极显发"
)

DEFAULT_SCAN_DIRS = ["src/quanquant/web", "docs"]
DEFAULT_EXTENSIONS = {".html", ".js", ".css", ".py", ".md"}

HELP_TEXT = __doc__


def collect_default_files() -> list[Path]:
    files: list[Path] = []
    for dirname in DEFAULT_SCAN_DIRS:
        root = Path(dirname)
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix in DEFAULT_EXTENSIONS:
                files.append(path)
    return files


def scan_file(path: Path) -> list[tuple[str, int, str, str]]:
    hits: list[tuple[str, int, str, str]] = []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return hits
    for lineno, line in enumerate(text.splitlines(), start=1):
        seen_on_line: set[str] = set()
        for ch in line:
            if ch in SIMPLIFIED_ONLY_CHARS and ch not in seen_on_line:
                seen_on_line.add(ch)
                snippet = line.strip()[:60]
                hits.append((str(path), lineno, ch, snippet))
    return hits


def main(argv: list[str]) -> int:
    if argv and argv[0] in ("-h", "--help"):
        print(HELP_TEXT)
        return 0

    if argv:
        targets = [Path(a) for a in argv]
    else:
        targets = collect_default_files()

    all_hits: list[tuple[str, int, str, str]] = []
    scanned = 0
    for f in targets:
        if not f.exists() or not f.is_file():
            continue
        scanned += 1
        all_hits.extend(scan_file(f))

    for path, lineno, ch, snippet in all_hits:
        print(f"{path}:{lineno}: {ch} ({snippet})")

    if all_hits:
        print(f"::error::發現 {len(all_hits)} 處疑似簡體字，共 {scanned} 個檔案被掃描。", file=sys.stderr)
        return 1

    print(f"check_simplified: 掃描 {scanned} 個檔案，未發現簡體字。")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
