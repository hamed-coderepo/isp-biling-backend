import os
import pandas as pd
from google.cloud import bigquery


def get_bq_client():
    project = os.getenv('BQ_PROJECT') or None
    return bigquery.Client(project=project)


def get_bq_table_id():
    project = os.getenv('BQ_PROJECT')
    dataset = os.getenv('BQ_DATASET')
    table = os.getenv('BQ_TABLE')
    if not (project and dataset and table):
        raise RuntimeError('BigQuery config missing: BQ_PROJECT, BQ_DATASET, BQ_TABLE')
    return f"{project}.{dataset}.{table}"


def run_bq_report_query(
    creators,
    limit=1000,
    date_op=None,
    date_value=None,
    date_start=None,
    date_end=None,
    service_status=None,
):
    table_id = get_bq_table_id()
    client = get_bq_client()

    if creators is None:
        creators_list = []
    elif isinstance(creators, (list, tuple, set)):
        creators_list = [str(c).strip().lower() for c in creators if c is not None and str(c).strip()]
    else:
        creators_list = [str(creators).strip().lower()]

    where_clauses = []
    params = []
    if creators_list:
        where_clauses.append("LOWER(TRIM(rs_username)) IN UNNEST(@creator_list)")
        params.insert(0, bigquery.ArrayQueryParameter('creator_list', 'STRING', creators_list))

    if service_status and str(service_status).strip().upper() != 'NONE':
        status_norm = str(service_status).strip().lower()
        where_clauses.append("LOWER(TRIM(ServiceStatus)) = @service_status")
        params.append(bigquery.ScalarQueryParameter('service_status', 'STRING', status_norm))

    if date_op in {'EXACT', '='} and date_value:
        where_clauses.append("CreateDate = @date_value")
        params.append(bigquery.ScalarQueryParameter('date_value', 'DATE', date_value))
    elif date_op in {'<', '>', '<=', '>='} and date_value:
        where_clauses.append(f"CreateDate {date_op} @date_value")
        params.append(bigquery.ScalarQueryParameter('date_value', 'DATE', date_value))
    elif date_op == 'BETWEEN' and date_start and date_end:
        where_clauses.append("CreateDate BETWEEN @date_start AND @date_end")
        params.append(bigquery.ScalarQueryParameter('date_start', 'DATE', date_start))
        params.append(bigquery.ScalarQueryParameter('date_end', 'DATE', date_end))

    if limit and int(limit) > 0:
        params.append(bigquery.ScalarQueryParameter('limit', 'INT64', int(limit)))

    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    limit_clause = ""
    if limit and int(limit) > 0:
        limit_clause = "LIMIT @limit"

    query = f"""
SELECT
    id,
    CreateDate,
    UserServiceID,
    rs_username,
    rs_name,
    ServiceName,
    username,
    ServiceStatus,
    ServicePrice,
    Package,
    StartDate,
    EndDate
FROM `{table_id}`
{where_clause}
ORDER BY rs_username ASC, UserServiceID ASC
{limit_clause}
"""
    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
    rows = [dict(r) for r in job.result()]
    return pd.DataFrame(rows), table_id
