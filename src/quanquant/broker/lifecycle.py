"""下單子系統 lifespan 輔助（Task 8）：shutdown sentinel（round3 #17）+ confirm token 定期
清理（round3 #16）。刻意獨立於 web/app.py，讓這兩段邏輯可以不經 FastAPI/真 event loop 依賴
直接單元測試（用假 order_service/inbox_worker）。
"""
import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timezone

from sqlmodel import Session

from quanquant.broker import repository as brepo

log = logging.getLogger(__name__)


async def shutdown_order_subsystem(
    *, order_service, inbox_worker, state, timeout: float = 5.0,
) -> bool:
    """round3 #17：原子關 ingress（先斷線登出，斷線後底層才不會再有新的 native callback
    落地）→ 等 `RawInboxWorker` 把已經落地的 batch 真正處理完（drain）。全程有 timeout，
    逾時回 False 並把 `state` 標成 unhealthy——**不宣稱**背景 thread/native 呼叫已經真的停止
    （asyncio 沒辦法砍掉正在跑的 native thread，只能不再等它）；逾時未處理完的資料仍安全
    留在 DB（RawInbox durable spool），不會遺失，下次啟動 worker 會自然撿起繼續處理。

    `order_service`/`inbox_worker`/`state` 允許為 None（下單子系統本來就沒啟用時的
    no-op），呼叫端（web/app.py lifespan）不需要另外判斷。
    """
    ok = True

    if order_service is not None:
        try:
            async with asyncio.timeout(timeout):
                await order_service.close()
        except TimeoutError:
            ok = False
            log.error("shutdown_order_subsystem: order_service.close() 逾時（ingress 未確定關閉）")
        except Exception as exc:
            ok = False
            log.error("shutdown_order_subsystem: order_service.close() 失敗: %s", exc)

    if inbox_worker is not None:
        drained = await inbox_worker.stop_and_drain(timeout=timeout)
        if not drained:
            ok = False

    if not ok and state is not None:
        state.mark_unhealthy("shutdown 逾時或失敗，下單子系統可能仍有背景工作未完成")

    return ok


async def run_confirm_token_cleanup(
    session_factory: Callable[[], Session], *, interval: float,
) -> None:
    """round3 #16：定期清理過期/已消費的 ConfirmToken，避免 DB 無界成長。無限迴圈協程，
    供 lifespan `create_task`；單次清理失敗不中止迴圈（背景維護工作，下一輪再試）。"""
    while True:
        try:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            with session_factory() as session:
                removed = brepo.cleanup_expired_confirm_tokens(session, now=now)
                session.commit()
                if removed:
                    log.info("confirm token 清理：移除 %d 筆過期/已消費的列", removed)
        except Exception as exc:
            log.warning("confirm token 清理失敗（下一輪再試）: %s", exc)
        await asyncio.sleep(interval)
