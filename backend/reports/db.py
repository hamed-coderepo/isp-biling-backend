import os
import pandas as pd
import pymysql

from pymysql.cursors import DictCursor
from maria_cache.models import (
    Center,
    CenterVispAccess,
    Reseller,
    ResellerPermit,
    Service,
    ServiceResellerAccess,
    ServiceVispAccess,
    Status,
    StatusResellerAccess,
    StatusVispAccess,
    Supporter,
    Visp,
)


def _parse_sources():
    sources_raw = os.getenv('MARIA_SOURCES', '').strip()
    if sources_raw:
        sources = []
        for item in sources_raw.split(';'):
            item = item.strip()
            if not item:
                continue
            parts = [p.strip() for p in item.split(',')]
            if len(parts) < 6:
                continue
            name, host, port, db, user, password = parts[:6]
            sources.append({
                'name': name or host,
                'host': host,
                'port': int(port),
                'db': db,
                'user': user,
                'password': password,
            })
        if sources:
            return sources

    return [{
        'name': os.getenv('MARIA_DB', 'default'),
        'host': os.getenv('MARIA_HOST', 'localhost'),
        'port': int(os.getenv('MARIA_PORT', 3306)),
        'db': os.getenv('MARIA_DB', ''),
        'user': os.getenv('MARIA_USER', 'root'),
        'password': os.getenv('MARIA_PASSWORD', ''),
    }]


def get_sources():
    return _parse_sources()


def get_conn(source_name=None):
    sources = _parse_sources()
    source = sources[0]
    if source_name:
        match = next((s for s in sources if s.get('name') == source_name), None)
        if match:
            source = match
    cfg = {
        'host': source['host'],
        'port': source['port'],
        'user': source['user'],
        'password': source['password'],
        'db': source['db'],
        'charset': 'utf8mb4',
        'cursorclass': DictCursor,
    }
    return pymysql.connect(**cfg)


def run_query(query, params=None, tables_priority=None, source_name=None):
    """Run a parameterized query against MariaDB.

    - `query` can contain a `{table_path}` placeholder which will be replaced by
      each candidate table name in `tables_priority` in order.
    - `params` should be a list/tuple of values for `%s` placeholders in the query.
    - Returns (DataFrame, used_table_name) or (empty DataFrame, None).
    """
    tables_priority = tables_priority or [None]
    last_error = None
    for table in tables_priority:
        q = query.format(table_path=table) if table else query
        try:
            with get_conn(source_name=source_name) as conn:
                with conn.cursor() as cur:
                    cur.execute(q, params or [])
                    rows = cur.fetchall()
                    if rows:
                        return pd.DataFrame(rows), table
        except Exception as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error
    return pd.DataFrame(), None


def _run_df(query, params=None, source_name=None):
    df, _ = run_query(query, params=params, source_name=source_name)
    return df


def _in_clause(values):
    placeholders = ','.join(['%s'] * len(values))
    return placeholders


def _df_to_choices(df, id_col, name_col):
    if df is None or df.empty:
        return []
    choices = []
    for _, row in df.iterrows():
        item_id = row.get(id_col)
        item_name = row.get(name_col)
        if item_id is None:
            continue
        choices.append((str(item_id), item_name))
    return choices


def fetch_reseller_by_username(reseller_username, source_name=None):
    if not reseller_username:
        return None
    norm = reseller_username.strip().lower()
    row = Reseller.objects.filter(
        source_name=source_name,
        name_norm=norm,
        is_enabled=True,
    ).first()
    if not row:
        return None
    return {
        'id': row.source_id,
        'name': row.name,
    }


def fetch_supporters(source_name=None):
    rows = Supporter.objects.filter(source_name=source_name, is_enabled=True).order_by('name')
    return [(str(r.source_id), r.name) for r in rows]


def fetch_general_permissions(reseller_id, source_name=None):
    return []


def fetch_visps_for_reseller(reseller_id, source_name=None):
    permit_rows = ResellerPermit.objects.filter(
        source_name=source_name,
        reseller_id=reseller_id,
        is_permit=True,
    )
    if not permit_rows.exists():
        return []

    visp_ids = sorted({int(v.visp_id) for v in permit_rows if v.visp_id is not None})
    has_all = 0 in visp_ids
    visp_ids = [v for v in visp_ids if v > 0]

    if not visp_ids and has_all:
        rows = Visp.objects.filter(source_name=source_name, is_enabled=True).order_by('name')
        return [(str(v.source_id), v.name) for v in rows]

    if not visp_ids:
        return []
    rows = Visp.objects.filter(
        source_name=source_name,
        source_id__in=visp_ids,
        is_enabled=True,
    ).order_by('name')
    return [(str(v.source_id), v.name) for v in rows]


def fetch_allowed_services(reseller_id, visp_ids, source_name=None):
    if not visp_ids:
        return []
    reseller_set = set(ServiceResellerAccess.objects.filter(
        source_name=source_name,
        reseller_id=reseller_id,
        checked=True,
    ).values_list('service_id', flat=True))
    visp_set = set(ServiceVispAccess.objects.filter(
        source_name=source_name,
        visp_id__in=visp_ids,
        checked=True,
    ).values_list('service_id', flat=True))

    rows = Service.objects.filter(
        source_name=source_name,
        is_enabled=True,
        is_deleted=False,
    ).order_by('name')

    choices = []
    for service in rows:
        reseller_ok = service.reseller_access == 'All' or service.source_id in reseller_set
        visp_ok = service.visp_access == 'All' or service.source_id in visp_set
        if reseller_ok and visp_ok:
            choices.append((str(service.source_id), service.name))
    return choices


def fetch_allowed_statuses(reseller_id, visp_ids, source_name=None):
    if not visp_ids:
        return []
    reseller_set = set(StatusResellerAccess.objects.filter(
        source_name=source_name,
        reseller_id=reseller_id,
        checked=True,
    ).values_list('status_id', flat=True))
    visp_set = set(StatusVispAccess.objects.filter(
        source_name=source_name,
        visp_id__in=visp_ids,
        checked=True,
    ).values_list('status_id', flat=True))

    rows = Status.objects.filter(
        source_name=source_name,
        is_enabled=True,
    ).order_by('name')

    choices = []
    for status in rows:
        reseller_ok = status.reseller_access == 'All' or status.source_id in reseller_set
        visp_ok = status.visp_access == 'All' or status.source_id in visp_set
        if reseller_ok and visp_ok:
            choices.append((str(status.source_id), status.name))
    return choices


def fetch_allowed_centers(visp_ids, source_name=None):
    if not visp_ids:
        return []
    visp_set = set(CenterVispAccess.objects.filter(
        source_name=source_name,
        visp_id__in=visp_ids,
        checked=True,
    ).values_list('center_id', flat=True))

    rows = Center.objects.filter(
        source_name=source_name,
        is_enabled=True,
    ).order_by('name')

    choices = []
    for center in rows:
        visp_ok = center.visp_access == 'All' or center.source_id in visp_set
        if visp_ok:
            choices.append((str(center.source_id), center.name))
    return choices


def fetch_allowed_packages(reseller_id, service_ids, source_name=None):
    reseller_query = (
        "SELECT DISTINCT Package_Id "
        "FROM Hreseller_packageaccess "
        "WHERE Reseller_Id = %s "
        "AND (Checked = 'Yes' OR Checked = 1)"
    )
    reseller_df = _run_df(reseller_query, params=[reseller_id], source_name=source_name)
    package_ids = []
    if not reseller_df.empty:
        package_ids = [int(v) for v in reseller_df['Package_Id'].tolist() if v is not None]
    if not package_ids:
        return []

    package_placeholders = _in_clause(package_ids)
    query = (
        "SELECT Package_Id, PackageName "
        "FROM Hpackage "
        f"WHERE Package_Id IN ({package_placeholders}) "
        "AND (ISEnable = 'Yes' OR ISEnable = 1) "
        "ORDER BY PackageName"
    )
    df = _run_df(query, params=package_ids, source_name=source_name)
    return _df_to_choices(df, 'Package_Id', 'PackageName')
