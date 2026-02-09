import datetime
import re
import secrets

from .db import get_conn


class UserCreateError(RuntimeError):
    pass


def _fetch_template_user(cur, reseller_id):
    cur.execute(
        "SELECT * FROM Huser WHERE Reseller_Id=%s ORDER BY User_Id DESC LIMIT 1",
        (reseller_id,),
    )
    row = cur.fetchone()
    if row:
        return row
    cur.execute("SELECT * FROM Huser ORDER BY User_Id DESC LIMIT 1")
    row = cur.fetchone()
    if not row:
        raise UserCreateError("No template user found in Huser.")
    return row


def _get_next_suffix(cur, prefix):
    if prefix:
        regex = f"^{re.escape(prefix)}[0-9]+$"
        start_pos = len(prefix) + 1
    else:
        regex = r"^[0-9]+$"
        start_pos = 1

    cur.execute(
        "SELECT MAX(CAST(SUBSTRING(Username, %s) AS UNSIGNED)) AS max_suffix "
        "FROM Huser WHERE Username REGEXP %s",
        (start_pos, regex),
    )
    row = cur.fetchone() or {}
    max_suffix = row.get("max_suffix") or 0
    return int(max_suffix) + 1


def create_users(payload):
    user_count = int(payload["user_count"])
    prefix = str(payload.get("username_prefix") or "").strip()
    if not prefix:
        raise UserCreateError("Username prefix is required.")
    if len(prefix) > 24:
        raise UserCreateError("Username prefix is too long (max 24 characters).")

    service_id = int(payload["service_id"])
    reseller_id = int(payload["reseller_id"])
    source_name = payload.get("server_name")
    visp_id = int(payload["visp_id"])
    center_id = int(payload["center_id"])
    supporter_id = int(payload["supporter_id"])
    status_id = int(payload["status_id"])

    created_rows = []
    now = datetime.datetime.now()
    batch_name = f"AddUser-{now.strftime('%Y/%m/%d %H:%M:%S')}-N={user_count}"

    conn = get_conn(source_name=source_name)
    try:
        with conn.cursor() as cur:
            template = _fetch_template_user(cur, reseller_id)
            next_suffix = _get_next_suffix(cur, prefix)

            cur.execute(
                "INSERT INTO Hbatchprocess ("
                "From_User_Index, To_User_Index, BatchProcessName, CDT, StartDT, EndDT, "
                "Creator_Id, SessionID, ClientIP, BatchItem, Option1, Option2, Option3, Option4, "
                "CompletedCount, BatchComment, BatchState"
                ") VALUES ("
                "%s, %s, %s, NOW(), NOW(), NOW(), %s, '', 0, 'AddUser', 'AddUser', %s, '', '', 0, '', 'InProgress'"
                ")",
                (next_suffix, next_suffix + user_count - 1, batch_name, reseller_id, service_id),
            )
            batch_id = cur.lastrowid

            for offset in range(user_count):
                suffix = next_suffix + offset
                suffix_str = str(suffix)
                username = f"{prefix}{suffix_str}"
                if len(username) > 32:
                    raise UserCreateError("Username exceeds 32 characters.")
                password = str(secrets.randbelow(900000000) + 100000000)

                user_row = dict(template)
                user_row["User_ServiceBase_Id"] = 0
                user_row["Reseller_Id"] = reseller_id
                user_row["Visp_Id"] = visp_id
                user_row["Center_Id"] = center_id
                user_row["Supporter_Id"] = supporter_id
                user_row["Status_Id"] = status_id
                user_row["UserCDT"] = now
                user_row["Username"] = username
                user_row["Pass"] = password
                user_row["StatusBy_Id"] = reseller_id
                user_row["StatusDT"] = now

                columns = [c for c in user_row.keys() if c != "User_Id"]
                values = [user_row[c] for c in columns]
                placeholders = ",".join(["%s"] * len(columns))

                cur.execute(
                    f"INSERT INTO Huser ({','.join(columns)}) VALUES ({placeholders})",
                    values,
                )
                user_id = cur.lastrowid

                cur.execute(
                    "INSERT INTO Huser_servicebase ("
                    "User_Id, Creator_Id, Service_Id, CDT, StartDate, EndDate, ServiceStatus, PayPlan, "
                    "ServicePrice, InstallmentNo, InstallmentPeriod, InstallmentFirstCash"
                    ") VALUES ("
                    "%s, %s, %s, NOW(), '0000-00-00', '0000-00-00', 'Pending', 'PrePaid', 0, 0, 0, 'No'"
                    ")",
                    (user_id, reseller_id, service_id),
                )
                user_service_id = cur.lastrowid

                cur.execute(
                    "UPDATE Huser SET User_ServiceBase_Id=%s WHERE User_Id=%s",
                    (user_service_id, user_id),
                )

                cur.execute(
                    "INSERT INTO Hbatchprocess_users (BatchProcess_Id, User_Id, BatchItemState, BatchItemDT, BatchItemComment) "
                    "VALUES (%s, %s, 'Done', NOW(), '')",
                    (batch_id, user_id),
                )

                created_rows.append({
                    "username": username,
                    "password": password,
                    "user_id": user_id,
                    "user_service_id": user_service_id,
                })

            cur.execute(
                "UPDATE Hbatchprocess SET CompletedCount=%s, BatchState='Done', StartDT=NOW(), EndDT=NOW() "
                "WHERE BatchProcess_Id=%s",
                (len(created_rows), batch_id),
            )

        conn.commit()
        return {
            "batch_id": batch_id,
            "batch_name": batch_name,
            "created": created_rows,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
