"""CSV / Excel export of trades and performance stats."""
import csv
import io
from datetime import datetime

from openpyxl import Workbook

from quanquant.journal.schemas import split_tags
from quanquant.stats.metrics import Metrics, StatsResult

TRADE_HEADERS = [
    "ID", "商品", "方向", "開倉時間", "開倉價", "平倉時間", "平倉價",
    "停損價", "停利策略", "口數", "每點價值", "手續費", "損益", "手動損益", "標籤", "備註",
]


def _fmt_dt(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M") if value else ""


def _trade_cells(t: object, *, numeric: bool) -> list:
    """Row for one trade. numeric=True keeps numbers as float for Excel cells."""
    def num(v):
        if v is None:
            return "" if not numeric else None
        return float(v) if numeric else str(v)

    return [
        t.id,
        t.symbol,
        "多" if t.direction == "long" else "空",
        _fmt_dt(t.entry_time),
        num(t.entry_price),
        _fmt_dt(t.exit_time),
        num(t.exit_price),
        num(t.stop_loss_price),
        t.take_profit_strategy or "",
        t.size,
        num(t.point_value),
        num(t.fee),
        num(t.pnl),
        "是" if t.pnl_is_manual else "",
        " ".join(split_tags(t.tags)),
        t.note or "",
    ]


def to_csv_bytes(trades: list) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(TRADE_HEADERS)
    for t in trades:
        writer.writerow(_trade_cells(t, numeric=False))
    # utf-8-sig so Excel opens Chinese headers correctly
    return buf.getvalue().encode("utf-8-sig")


def _metrics_rows(label: str, m: Metrics) -> list[list]:
    def f(v):
        return float(v) if v is not None else ""

    return [
        [label, ""],
        ["交易次數", m.count],
        ["總損益", f(m.total_pnl)],
        ["勝率", round(m.win_rate, 4)],
        ["獲利筆數", m.wins],
        ["虧損筆數", m.losses],
        ["平均獲利", f(m.avg_win)],
        ["平均虧損", f(m.avg_loss)],
        ["最大單筆獲利", f(m.max_win)],
        ["最大單筆虧損", f(m.max_loss)],
        ["獲利因子", round(m.profit_factor, 4) if m.profit_factor is not None else ""],
        ["最大回撤", f(m.max_drawdown)],
        ["", ""],
    ]


def to_xlsx_bytes(trades: list, stats: StatsResult) -> bytes:
    wb = Workbook()

    ws1 = wb.active
    ws1.title = "交易明細"
    ws1.append(TRADE_HEADERS)
    for t in trades:
        ws1.append(_trade_cells(t, numeric=True))

    ws2 = wb.create_sheet("績效統計")
    for row in _metrics_rows("整體", stats.overall):
        ws2.append(row)
    if stats.by_tag:
        ws2.append(["— 依標籤 —", ""])
        for tag, m in stats.by_tag.items():
            for row in _metrics_rows(f"標籤: {tag}", m):
                ws2.append(row)
    if stats.by_symbol:
        ws2.append(["— 依商品 —", ""])
        for sym, m in stats.by_symbol.items():
            for row in _metrics_rows(f"商品: {sym}", m):
                ws2.append(row)

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()
