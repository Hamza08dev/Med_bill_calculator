# scripts/setup_neo4j_user.py
from neo4j import GraphDatabase
import argparse

def ident(name: str) -> str:
    # minimal identifier safety – backtick quoting
    if not name or any(c in name for c in "`"):
        raise ValueError("Bad username")
    return f"`{name}`"

def ensure_user(uri, admin_user, admin_pass, new_user, new_pass, roles):
    driver = GraphDatabase.driver(uri, auth=(admin_user, admin_pass))
    with driver.session() as s:
        # does the user exist?
        rec = s.run(
            "SHOW USERS YIELD user WHERE user = $u RETURN user",
            u=new_user,
        ).single()

        if rec is None:
            s.run(
                f"CREATE USER {ident(new_user)} "
                "SET PASSWORD $p CHANGE NOT REQUIRED",
                p=new_pass,
            )
            print(f"Created user: {new_user}")
        else:
            # set/refresh password
            s.run(
                f"ALTER USER {ident(new_user)} "
                "SET PASSWORD $p CHANGE NOT REQUIRED",
                p=new_pass,
            )
            print(f"Updated password for user: {new_user}")

        # grant roles
        for r in roles:
            s.run(f"GRANT ROLE {ident(r)} TO {ident(new_user)}")
        print(f"Granted roles {roles} to {new_user}")
    driver.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--admin-user", required=True)
    ap.add_argument("--admin-pass", required=True)
    ap.add_argument("--new-user", required=True)
    ap.add_argument("--new-pass", required=True)
    ap.add_argument("--roles", default="reader", help="comma-separated (e.g., reader,editor or admin)")
    args = ap.parse_args()
    roles = [r.strip() for r in args.roles.split(",") if r.strip()]
    ensure_user(args.uri, args.admin_user, args.admin_pass, args.new_user, args.new_pass, roles)
