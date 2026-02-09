import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "isp_report.settings")

from reports.db import get_conn

source_name = "RS01"

tables = [
    "Hreseller",
    "Hpermititem",
    "Hreseller_permit",
    "Hvisp",
    "Hservice",
    "Hservice_reselleraccess",
    "Hservice_vispaccess",
    "Hstatus",
    "Hstatus_reselleraccess",
    "Hstatus_vispaccess",
    "Hcenter",
    "Hcenter_vispaccess",
    "Hpackage",
    "Hreseller_packageaccess",
]


def safe_table(table):
    return "`%s`" % table.replace("`", "")


with get_conn(source_name=source_name) as conn:
    with conn.cursor() as cur:
        for table in tables:
            print("\n===", table, "===")
            try:
                cur.execute("SHOW COLUMNS FROM %s" % safe_table(table))
                cols = cur.fetchall()
                print("columns:")
                for col in cols:
                    print(" - {Field} ({Type})".format(**col))
                cur.execute("SELECT COUNT(*) AS c FROM %s" % safe_table(table))
                count = cur.fetchone().get("c")
                print("count:", count)
                cur.execute("SELECT * FROM %s LIMIT 5" % safe_table(table))
                rows = cur.fetchall()
                if rows:
                    print("sample rows (keys only):", list(rows[0].keys()))
                else:
                    print("sample rows: none")
            except Exception as exc:
                print("error:", exc)
                continue
