import argparse
import json


def main():
    parser = argparse.ArgumentParser(description="秋窗客流分析账本")
    parser.add_argument("command", choices=["demo", "serve", "check", "worker"])
    parser.add_argument("--database", default="autumn-ledger.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--owner", default=None, help="工作进程标识，默认 worker-<进程号>")
    parser.add_argument("--lease-seconds", type=int, default=30, help="作业租约秒数")
    parser.add_argument("--poll-seconds", type=float, default=1.0, help="空闲轮询间隔秒数")
    parser.add_argument("--once", action="store_true", help="处理完当前可领取的作业后退出")
    args = parser.parse_args()
    if args.command == "demo":
        from .demo import run_demo
        print(json.dumps(run_demo(), ensure_ascii=False, sort_keys=True))
    elif args.command == "check":
        from .application import Ledger
        result = Ledger(args.database).status()
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        if not result["ok"]:
            raise SystemExit(1)
    elif args.command == "worker":
        import os
        import time
        from .application import Ledger
        ledger = Ledger(args.database)
        owner = args.owner or f"worker-{os.getpid()}"
        while True:
            try:
                processed = ledger.jobs.work(owner, args.lease_seconds)
            except KeyboardInterrupt:
                break
            if args.once:
                break
            if processed == 0:
                time.sleep(args.poll_seconds)
    else:
        import uvicorn
        from .api.server import create_app
        uvicorn.run(create_app(args.database), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
