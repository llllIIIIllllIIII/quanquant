"""User management CLI: `quanquant-user bootstrap|create|reset-password|list`.

bootstrap = create the FIRST admin and claim all pre-account data (user_id IS
NULL trades/alerts; legacy chart_states indicators/drawings copied — not moved —
to user_chart_states). Refuses to run when an admin already exists.
"""
import argparse
import getpass
import sys

from sqlalchemy import update
from sqlmodel import Session, select

from quanquant.auth import service
from quanquant.db.engine import get_engine, init_db
from quanquant.db.models import Alert, ChartState, Trade, User, UserChartState


def bootstrap_admin(db: Session, username: str, password: str) -> User:
    existing_admin = db.exec(select(User).where(User.role == "admin")).first()
    if existing_admin is not None:
        print(f"已存在 admin（{existing_admin.username}），bootstrap 拒絕重跑", file=sys.stderr)
        raise SystemExit(1)

    admin = service.create_user(db, username, password, role="admin")

    db.execute(update(Trade).where(Trade.user_id.is_(None)).values(user_id=admin.id))
    db.execute(update(Alert).where(Alert.user_id.is_(None)).values(user_id=admin.id))

    legacy = db.exec(
        select(ChartState).where(ChartState.kind.in_(("indicators", "drawings")))  # type: ignore[union-attr]
    ).all()
    for row in legacy:
        dup = db.exec(
            select(UserChartState).where(
                UserChartState.user_id == admin.id,
                UserChartState.symbol == row.symbol,
                UserChartState.kind == row.kind,
            )
        ).first()
        if dup is None:
            db.add(UserChartState(
                user_id=admin.id, symbol=row.symbol, kind=row.kind, payload=row.payload
            ))
    db.commit()
    return admin


def _prompt_password(args) -> str:
    if args.password:
        return args.password
    pw = getpass.getpass("密碼: ")
    if getpass.getpass("再輸入一次: ") != pw:
        print("兩次輸入不一致", file=sys.stderr)
        raise SystemExit(1)
    return pw


def main() -> None:
    parser = argparse.ArgumentParser(prog="quanquant-user")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_boot = sub.add_parser("bootstrap", help="建第一個 admin 並認領舊資料")
    p_boot.add_argument("username")
    p_boot.add_argument("--password", help="未提供則互動輸入")

    p_create = sub.add_parser("create", help="建一般帳號")
    p_create.add_argument("username")
    p_create.add_argument("--display-name")
    p_create.add_argument("--role", choices=("admin", "user"), default="user")
    p_create.add_argument("--password")

    p_reset = sub.add_parser("reset-password", help="重設密碼（bump token_version）")
    p_reset.add_argument("username")
    p_reset.add_argument("--password")

    sub.add_parser("list", help="列出帳號")

    args = parser.parse_args()
    init_db()
    with Session(get_engine()) as db:
        if args.cmd == "bootstrap":
            admin = bootstrap_admin(db, args.username, _prompt_password(args))
            print(f"admin '{admin.username}' 建立完成，舊資料已認領")
        elif args.cmd == "create":
            user = service.create_user(
                db, args.username, _prompt_password(args),
                display_name=args.display_name, role=args.role,
            )
            print(f"使用者 '{user.username}'（{user.role}）建立完成")
        elif args.cmd == "reset-password":
            user = service.get_by_username(db, args.username)
            if user is None:
                print(f"找不到使用者 '{args.username}'", file=sys.stderr)
                raise SystemExit(1)
            service.reset_password(db, user, _prompt_password(args))
            print("密碼已重設（所有裝置需重新登入）")
        elif args.cmd == "list":
            for u in service.list_users(db):
                flag = "" if u.is_active else "（停用）"
                print(f"{u.id}\t{u.username}\t{u.role}\t{u.display_name}{flag}")
