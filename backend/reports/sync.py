import os
import logging
import tempfile
import uuid
from google.cloud import bigquery
import pandas as pd
import pymysql

from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)


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

    # fallback to single-source env vars
    return [{
        'name': os.getenv('MARIA_DB', 'default'),
        'host': os.getenv('MARIA_HOST', 'localhost'),
        'port': int(os.getenv('MARIA_PORT', 3306)),
        'db': os.getenv('MARIA_DB', ''),
        'user': os.getenv('MARIA_USER', 'root'),
        'password': os.getenv('MARIA_PASSWORD', ''),
    }]


def _fetch_maria_rows(source, limit=0):
    logger.info("Sync: fetching rows from %s (%s:%s/%s)", source.get('name'), source.get('host'), source.get('port'), source.get('db'))
    query = """
SELECT
    DATE(TName.CDT) AS CreateDate,
    TName.Creator_Id AS rs_userid,
    Hrc.ResellerName AS rs_username,
    Hrc.ResellerName AS rs_name,
    TName.User_ServiceBase_Id AS UserServiceID,
    Hu.Username AS username,
    Hse.ServiceName AS ServiceName,
    TName.ServicePrice AS ServicePrice,
    CASE
        WHEN Hse.STrA IS NULL THEN NULL
        ELSE ROUND(Hse.STrA / 1073741824, 2)
    END AS Package,
    TName.ServiceStatus AS ServiceStatus,
    DATE_FORMAT(NULLIF(TName.StartDate, '0000-00-00'), '%Y-%m-%d') AS StartDate,
    DATE_FORMAT(NULLIF(TName.EndDate, '0000-00-00'), '%Y-%m-%d') AS EndDate
FROM Huser_servicebase TName
JOIN Huser Hu ON TName.User_Id = Hu.User_Id
LEFT JOIN Hreseller Hrc ON TName.Creator_Id = Hrc.Reseller_Id
LEFT JOIN Hservice Hse ON TName.Service_Id = Hse.Service_Id
ORDER BY TName.CDT DESC
"""
    if limit and limit > 0:
        query += f"\nLIMIT {int(limit)}"

    conn = pymysql.connect(
        host=source['host'],
        port=source['port'],
        user=source['user'],
        password=source['password'],
        db=source['db'],
        charset='utf8mb4',
        cursorclass=DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
            if not df.empty:
                df['rs_name'] = source['name']
                df['id'] = range(1, len(df) + 1)
                ordered_cols = [
                    'id',
                    'CreateDate',
                    'rs_userid',
                    'rs_username',
                    'rs_name',
                    'UserServiceID',
                    'username',
                    'ServiceName',
                    'ServicePrice',
                    'Package',
                    'ServiceStatus',
                    'StartDate',
                    'EndDate',
                ]
                df = df[ordered_cols]
            logger.info("Sync: fetched %s rows from %s", len(df), source.get('name'))
            return df
    except Exception:
        logger.exception("Sync: failed to fetch rows from %s", source.get('name'))
        raise
    finally:
        conn.close()


def sync_maria_to_bigquery(limit=0, write_disposition='WRITE_TRUNCATE'):
    project = os.getenv('BQ_PROJECT')
    dataset = os.getenv('BQ_DATASET')
    table = os.getenv('BQ_TABLE')
    location = os.getenv('BQ_LOCATION', 'US')
    if not (project and dataset and table):
        raise RuntimeError('BigQuery config missing: BQ_PROJECT, BQ_DATASET, BQ_TABLE')

    table_id = f"{project}.{dataset}.{table}"

    sources = _parse_sources()
    logger.info("Sync: starting BigQuery load to %s", table_id)
    all_dfs = []
    for source in sources:
        try:
            df = _fetch_maria_rows(source, limit=limit)
            if not df.empty:
                all_dfs.append(df)
        except Exception:
            logger.exception("Sync: source failed: %s", source.get('name'))

    if not all_dfs:
        logger.warning("Sync: no data fetched from any source")
        return 0

    df = pd.concat(all_dfs, ignore_index=True)

    # sort by reseller username then date for easier grouping in BigQuery
    sort_cols = [c for c in ['rs_username', 'UserServiceID', 'CreateDate'] if c in df.columns]
    if sort_cols:
        ascending = [True] * len(sort_cols)
        if 'CreateDate' in sort_cols:
            ascending[sort_cols.index('CreateDate')] = False
        df = df.sort_values(by=sort_cols, ascending=ascending, na_position='last')

    df = df.reset_index(drop=True)
    df['id'] = range(1, len(df) + 1)

    ordered_cols = [
        'id',
        'CreateDate',
        'rs_userid',
        'rs_username',
        'rs_name',
        'UserServiceID',
        'username',
        'ServiceName',
        'ServicePrice',
        'Package',
        'ServiceStatus',
        'StartDate',
        'EndDate',
    ]
    ordered_cols = [c for c in ordered_cols if c in df.columns]
    if ordered_cols:
        df = df[ordered_cols]

    client = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
    )

    with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False, encoding='utf-8') as tmp:
        df.to_csv(tmp.name, index=False)
        tmp.flush()
        tmp.seek(0)
        with open(tmp.name, 'rb') as fh:
            load_job = client.load_table_from_file(
                fh,
                table_id,
                job_config=job_config,
                location=location,
            )
            load_job.result()
            logger.info("Sync: loaded %s rows into %s", len(df), table_id)

    return len(df)
