"""agent 本機 GUI（agent 設定精靈，M2）：程序內 FastAPI/uvicorn，僅 bind 127.0.0.1，
供瀏覽器一次性 bootstrap 交換＋設定精靈（Task 9 `/setup`）＋狀態儀表板（Task 10
`/status`）使用。`coordinator.py` 是唯一的生命週期入口（`run_gui()`）；`security.py`
是本機安全邊界（bootstrap exchange、session cookie、Host/Origin 檢查、安全標頭）。
"""
