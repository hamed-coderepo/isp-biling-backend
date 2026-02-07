import os
import datetime
import pandas as pd
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from .forms import FilterForm
from .models import ResellerProfile
from fpdf import FPDF

from .db import run_query
from .bq import run_bq_report_query


def safe_text(text):
    try:
        return str(text).encode('latin-1', 'ignore').decode('latin-1')
    except Exception:
        return ''


class PDF(FPDF):
    def header(self):
        self.set_fill_color(220, 220, 220)
        self.set_text_color(0)


def export_df_to_pdf(df):
    if df is None or df.empty:
        return None

    pdf = PDF(orientation='L')
    pdf.set_auto_page_break(auto=True, margin=2)
    pdf.add_page()
    try:
        pdf.set_font("Arial", size=8)
    except Exception:
        pdf.set_font("helvetica", size=8)

    # column width calculation based on content
    cols = list(df.columns)
    usable_width = 270  # A4 landscape width (297) minus margins
    min_width = 18
    max_width = 60
    line_height = 6

    def _safe_str(value):
        if value is None:
            return ""
        return safe_text(value)

    # estimate width for each column based on max string width
    col_widths = []
    for c in cols:
        values = df[c].astype(str).fillna('')
        sample = list(values.head(200))
        max_text = max([c] + sample, key=lambda s: pdf.get_string_width(str(s)))
        w = pdf.get_string_width(str(max_text)) + 6
        col_widths.append(max(min_width, min(max_width, w)))

    total_width = sum(col_widths)
    if total_width > usable_width:
        scale = usable_width / total_width
        col_widths = [max(min_width, w * scale) for w in col_widths]

    def _truncate(text, width):
        if pdf.get_string_width(text) <= width - 2:
            return text
        ellipsis = '...'
        max_w = max(0, width - pdf.get_string_width(ellipsis) - 2)
        trimmed = ''
        for ch in text:
            if pdf.get_string_width(trimmed + ch) > max_w:
                break
            trimmed += ch
        return trimmed + ellipsis

    # header (white background)
    pdf.set_fill_color(255, 255, 255)
    for i, c in enumerate(cols):
        header_text = _truncate(_safe_str(c), col_widths[i])
        pdf.cell(col_widths[i], line_height, header_text, border=1, align='C', fill=True)
    pdf.ln(line_height)

    for idx, row in df.iterrows():
        for i, c in enumerate(cols):
            cell_text = _truncate(_safe_str(row[c]), col_widths[i])
            pdf.cell(col_widths[i], line_height, cell_text, border=1)
        pdf.ln(line_height)

    return pdf.output(dest='S').encode('latin1')


@login_required
def report_view(request):
    form = FilterForm(request.POST or None)
    final_df = pd.DataFrame()
    info_tables = []
    show_results = False
    show_summary = False
    summary_rows = []
    summary_grand_total = None
    summary_grand_count = None

    # determine allowed creators for this user
    if hasattr(request.user, 'resellerprofile'):
        allowed_creators = [request.user.resellerprofile.reseller_name]
    else:
        allowed_creators = None  # admin or not assigned

    if request.method == 'POST' and form.is_valid():
        action = request.POST.get('action')
        creators_raw = form.cleaned_data.get('creators_raw')
        if creators_raw:
            creators_raw = creators_raw.replace('،', ',').replace('؛', ',').replace(';', ',')
        if creators_raw and [c.strip() for c in creators_raw.split(',') if c.strip()]:
            creators = [c.strip() for c in creators_raw.split(',') if c.strip()]
        else:
            if allowed_creators:
                creators = allowed_creators
            elif request.user.is_superuser:
                creators = [None]  # no filter for superusers
            else:
                creators = []

        if not creators:
            request.session['error'] = 'No reseller selected or assigned. Enter a creator name or ask admin to assign your reseller profile.'
            return render(request, 'report/report.html', {
                'form': form,
                'df': None,
                'info_tables': info_tables,
                'show_results': False,
                'error': request.session.pop('error', None)
            })

        total_dfs = []
        tables_priority = [t.strip() for t in os.getenv('BQ_TABLE_PRIORITY', '').split(',') if t.strip()]
        if not tables_priority:
            tables_priority = ['Huser_servicebase']
        limit = 0
        use_bq = os.getenv('REPORT_SOURCE', 'mariadb').lower() == 'bigquery'
        bq_creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '').strip()
        if use_bq:
            if bq_creds_path and not os.path.exists(bq_creds_path):
                local_keys = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'keys.json'))
                if os.path.exists(local_keys):
                    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = local_keys
                else:
                    use_bq = False
                    request.session['error'] = 'BigQuery credentials not configured. Falling back to MariaDB.'
            elif not bq_creds_path:
                local_keys = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'keys.json'))
                if os.path.exists(local_keys):
                    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = local_keys

        def _safe_filename(value):
            if value is None:
                return 'all'
            value = str(value)
            value = value.replace(' ', '-')
            return ''.join(ch for ch in value if ch.isalnum() or ch in {'-', '_'}).strip('-_') or 'all'

        def _date_str(value):
            if not value:
                return None
            if hasattr(value, 'isoformat'):
                return value.isoformat()
            return str(value)

        def _build_report_filename(ext, creators_list, filters):
            if creators_list is None:
                creator_part = 'all'
            elif len(creators_list) == 1:
                creator_part = _safe_filename(creators_list[0])
            else:
                creator_part = 'multi'

            parts = [creator_part]

            date_op = filters.get('date_op')
            date_value = _date_str(filters.get('date_value'))
            date_start = _date_str(filters.get('date_start'))
            date_end = _date_str(filters.get('date_end'))
            if date_op == 'EXACT' and date_value:
                parts.append(f"date-{_safe_filename(date_value)}")
            elif date_op == 'BETWEEN' and date_start and date_end:
                parts.append(f"date-{_safe_filename(date_start)}-to-{_safe_filename(date_end)}")

            serial_op = filters.get('serial_op')
            serial_value = filters.get('serial_value')
            serial_min = filters.get('serial_min')
            serial_max = filters.get('serial_max')
            if serial_op == 'NONE':
                pass
            elif serial_op == 'BETWEEN' and serial_min is not None and serial_max is not None:
                parts.append(f"serial-{serial_min}-to-{serial_max}")
            elif serial_op in {'=', '>', '<'} and serial_value is not None:
                op_map = {'=': 'eq', '>': 'gt', '<': 'lt'}
                parts.append(f"serial-{op_map.get(serial_op, 'eq')}-{serial_value}")

            name = 'report-' + '-'.join([p for p in parts if p])
            return f"{name}.{ext}"

        def _parse_date(value):
            if not value:
                return None
            if isinstance(value, datetime.date):
                return value
            try:
                return datetime.date.fromisoformat(str(value))
            except Exception:
                return None

        def _serialize_date(value):
            if not value:
                return None
            if hasattr(value, 'isoformat'):
                return value.isoformat()
            return str(value)

        if use_bq:
            try:
                bq_date_op = form.cleaned_data.get('date_op')
                bq_date_value = form.cleaned_data.get('date_value')
                bq_date_start = form.cleaned_data.get('date_start')
                bq_date_end = form.cleaned_data.get('date_end')

                if bq_date_op == 'NONE':
                    bq_date_op = None
                if bq_date_op in (None, '') and bq_date_start and bq_date_end:
                    bq_date_op = 'BETWEEN'
                if bq_date_op == 'EXACT' and not bq_date_value and bq_date_start and bq_date_end:
                    bq_date_op = 'BETWEEN'
                if bq_date_op == 'BETWEEN' and (not bq_date_start or not bq_date_end) and bq_date_value:
                    bq_date_op = 'EXACT'
                df, used_table = run_bq_report_query(
                    creators,
                    limit=limit,
                    date_op=bq_date_op,
                    date_value=bq_date_value,
                    date_start=bq_date_start,
                    date_end=bq_date_end,
                )
                if not df.empty:
                    total_dfs.append(df)
                    creators_label = ', '.join([c for c in creators if c]) if creators else 'all'
                    info_tables.append(f"{creators_label} ← {used_table}")
            except Exception as exc:
                request.session['error'] = str(exc)
        else:
            for creator in creators:
                query_base = """
SELECT
    TName.User_ServiceBase_Id AS RowID,
    IF(TName.Creator_Id = 0, '- User_From_Site -', Hrc.ResellerName) AS Creator,
    Hse.ServiceName AS ServiceName,
    Hu.Username AS Username,
    DATE_FORMAT(TName.CDT, '%%Y-%%m-%%d %%H:%%i:%%s') AS CreateDT,
    TName.ServiceStatus AS ServiceStatus,
    FORMAT(TName.ServicePrice, 0) AS ServicePrice,
    DATE_FORMAT(NULLIF(TName.StartDate, '0000-00-00'), '%%Y-%%m-%%d') AS StartDate,
    DATE_FORMAT(NULLIF(TName.EndDate, '0000-00-00'), '%%Y-%%m-%%d') AS EndDate,
    Hse.STrA AS PackageBytes,
    CASE
        WHEN Hse.STrA IS NULL THEN NULL
        ELSE ROUND(Hse.STrA / 1024, 2)
    END AS PackageValue
FROM {table_path} TName
JOIN Huser Hu ON TName.User_Id = Hu.User_Id
LEFT JOIN Hreseller Hrc ON TName.Creator_Id = Hrc.Reseller_Id
LEFT JOIN Hservice Hse ON TName.Service_Id = Hse.Service_Id
WHERE (TRIM(LOWER(Hrc.ResellerName)) = TRIM(LOWER(%s)) OR %s IS NULL)
ORDER BY TName.CDT DESC
LIMIT %s
"""
                params = [creator, creator, limit]
                try:
                    df, used_table = run_query(query_base, params=params, tables_priority=tables_priority)
                    if not df.empty:
                        total_dfs.append(df)
                        info_tables.append(f"{creator} ← {used_table}")
                except Exception as exc:
                    request.session['error'] = str(exc)
                    break

        show_results = True
        if total_dfs:
            final_df = pd.concat(total_dfs, ignore_index=True)
            final_df = final_df.where(pd.notnull(final_df), None)

            # Reorder columns for BigQuery reports
            if use_bq:
                desired_order = [
                    'id',
                    'CreateDate',
                    'UserServiceID',
                    'rs_username',
                    'rs_name',
                    'ServiceName',
                    'username',
                    'ServiceStatus',
                    'ServicePrice',
                    'Package',
                    'StartDate',
                    'EndDate',
                ]
                present = [c for c in desired_order if c in final_df.columns]
                remaining = [c for c in final_df.columns if c not in present]
                if present:
                    final_df = final_df[present + remaining]

            if 'Package' not in final_df.columns and 'PackageValue' in final_df.columns:
                final_df['Package'] = final_df['PackageValue']

            if 'Package' in final_df.columns:
                pkg_numeric = pd.to_numeric(final_df['Package'], errors='coerce')
                if pkg_numeric.notna().any() and pkg_numeric.max() >= 1024:
                    final_df['Package'] = (pkg_numeric / 1024).round(2)

            # build effective filters (reuse from session for downloads)
            filter_serial_post = (request.POST.get('filter_serial') or '').strip().lower()
            filter_date_post = (request.POST.get('filter_date') or '').strip().lower()
            filter_input_present = (
                form.cleaned_data.get('filter_serial')
                or form.cleaned_data.get('filter_date')
                or filter_serial_post in {'on', 'true', '1', 'yes', 'y'}
                or filter_date_post in {'on', 'true', '1', 'yes', 'y'}
                or any(
                    v is not None
                    for v in [
                        form.cleaned_data.get('serial_value'),
                        form.cleaned_data.get('serial_min'),
                        form.cleaned_data.get('serial_max'),
                        form.cleaned_data.get('sib_serial_value'),
                        form.cleaned_data.get('sib_serial_min'),
                        form.cleaned_data.get('sib_serial_max'),
                        form.cleaned_data.get('date_value'),
                        form.cleaned_data.get('date_start'),
                        form.cleaned_data.get('date_end'),
                    ]
                )
            )

            if filter_input_present:
                request.session['report_filters'] = {
                    'filter_serial': bool(
                        form.cleaned_data.get('filter_serial')
                        or filter_serial_post in {'on', 'true', '1', 'yes', 'y'}
                    ),
                    'serial_op': form.cleaned_data.get('serial_op'),
                    'serial_value': form.cleaned_data.get('serial_value'),
                    'serial_min': form.cleaned_data.get('serial_min'),
                    'serial_max': form.cleaned_data.get('serial_max'),
                    'sib_serial_op': form.cleaned_data.get('sib_serial_op'),
                    'sib_serial_value': form.cleaned_data.get('sib_serial_value'),
                    'sib_serial_min': form.cleaned_data.get('sib_serial_min'),
                    'sib_serial_max': form.cleaned_data.get('sib_serial_max'),
                    'filter_date': bool(
                        form.cleaned_data.get('filter_date')
                        or filter_date_post in {'on', 'true', '1', 'yes', 'y'}
                    ),
                    'date_op': form.cleaned_data.get('date_op'),
                    'date_value': _serialize_date(form.cleaned_data.get('date_value')),
                    'date_start': _serialize_date(form.cleaned_data.get('date_start')),
                    'date_end': _serialize_date(form.cleaned_data.get('date_end')),
                }
            else:
                if creators_raw:
                    request.session.pop('report_filters', None)

            session_filters = request.session.get('report_filters') or {}
            use_session_filters = (
                not filter_input_present
                and action in {'download_report', 'download_csv', 'download_summary_pdf'}
                and session_filters
            )

            effective_filters = form.cleaned_data
            if use_session_filters:
                effective_filters = {
                    **form.cleaned_data,
                    **session_filters,
                    'date_value': _parse_date(session_filters.get('date_value')),
                    'date_start': _parse_date(session_filters.get('date_start')),
                    'date_end': _parse_date(session_filters.get('date_end')),
                }

            # apply serial filter
            serial_op = effective_filters.get('serial_op')
            serial_value = effective_filters.get('serial_value')
            serial_min = effective_filters.get('serial_min')
            serial_max = effective_filters.get('serial_max')
            filter_serial_flag = effective_filters.get('filter_serial')
            filter_serial_enabled = (
                filter_serial_flag
                or any(v is not None for v in [serial_value, serial_min, serial_max])
            )

            if serial_op == 'NONE':
                filter_serial_enabled = False

            if filter_serial_enabled:
                serial_col = 'UserServiceID' if 'UserServiceID' in final_df.columns else 'RowID'
                if serial_col in final_df.columns:
                    if serial_op in (None, ''):
                        serial_op = '='
                    if serial_op == '=' and serial_value is None and serial_min is not None and serial_max is not None:
                        serial_op = 'BETWEEN'
                    elif serial_op == 'BETWEEN' and (serial_min is None or serial_max is None) and serial_value is not None:
                        serial_op = '='

                    serial_series = pd.to_numeric(final_df[serial_col], errors='coerce')
                    if serial_op == 'BETWEEN' and serial_min is not None and serial_max is not None:
                        final_df = final_df[serial_series.between(serial_min, serial_max)]
                    elif serial_op == '=' and serial_value is not None:
                        final_df = final_df[serial_series == serial_value]
                    elif serial_op == '>' and serial_value is not None:
                        final_df = final_df[serial_series > serial_value]
                    elif serial_op == '<' and serial_value is not None:
                        final_df = final_df[serial_series < serial_value]

            # apply date filter
            date_op = effective_filters.get('date_op')
            date_value = effective_filters.get('date_value')
            date_start = effective_filters.get('date_start')
            date_end = effective_filters.get('date_end')
            filter_date_flag = effective_filters.get('filter_date')
            filter_date_enabled = (
                filter_date_flag
                or any(v is not None for v in [date_value, date_start, date_end])
            )

            if date_op == 'NONE':
                filter_date_enabled = False

            if filter_date_enabled:
                date_col = 'CreateDT' if 'CreateDT' in final_df.columns else 'CreateDate'
                if date_col in final_df.columns:
                    if date_op in (None, ''):
                        date_op = 'EXACT'
                    if date_op == 'EXACT' and date_value is None and date_start and date_end:
                        date_op = 'BETWEEN'
                    elif date_op == 'BETWEEN' and (not date_start or not date_end) and date_value:
                        date_op = 'EXACT'

                    date_series = pd.to_datetime(final_df[date_col], errors='coerce')
                    if date_op == 'EXACT' and date_value:
                        final_df = final_df[date_series.dt.date == date_value]
                    elif date_op == 'BETWEEN' and date_start and date_end:
                        final_df = final_df[(date_series.dt.date >= date_start) & (date_series.dt.date <= date_end)]

            # apply SIB serial filter (UserServiceID only)
            sib_serial_op = effective_filters.get('sib_serial_op')
            sib_serial_value = effective_filters.get('sib_serial_value')
            sib_serial_min = effective_filters.get('sib_serial_min')
            sib_serial_max = effective_filters.get('sib_serial_max')

            sib_serial_enabled = any(v is not None for v in [sib_serial_value, sib_serial_min, sib_serial_max])
            if sib_serial_op == 'NONE':
                sib_serial_enabled = False

            if sib_serial_enabled and 'UserServiceID' in final_df.columns:
                if sib_serial_op in (None, ''):
                    sib_serial_op = '='
                if sib_serial_op == '=' and sib_serial_value is None and sib_serial_min is not None and sib_serial_max is not None:
                    sib_serial_op = 'BETWEEN'
                elif sib_serial_op == 'BETWEEN' and (sib_serial_min is None or sib_serial_max is None) and sib_serial_value is not None:
                    sib_serial_op = '='

                sib_series = pd.to_numeric(final_df['UserServiceID'], errors='coerce')
                if sib_serial_op == 'BETWEEN' and sib_serial_min is not None and sib_serial_max is not None:
                    final_df = final_df[sib_series.between(sib_serial_min, sib_serial_max)]
                elif sib_serial_op == '=' and sib_serial_value is not None:
                    final_df = final_df[sib_series == sib_serial_value]
                elif sib_serial_op == '>' and sib_serial_value is not None:
                    final_df = final_df[sib_series > sib_serial_value]
                elif sib_serial_op == '<' and sib_serial_value is not None:
                    final_df = final_df[sib_series < sib_serial_value]

            # sort rows grouped by reseller/creator and UserServiceID for easier review
            if 'Creator' in final_df.columns or 'rs_username' in final_df.columns:
                sort_cols = ['Creator'] if 'Creator' in final_df.columns else ['rs_username']
                ascending = [True]
                if 'UserServiceID' in final_df.columns:
                    sort_cols.append('UserServiceID')
                    ascending.append(True)
                elif 'RowID' in final_df.columns:
                    sort_cols.append('RowID')
                    ascending.append(True)
                else:
                    date_sort_col = 'CreateDT' if 'CreateDT' in final_df.columns else 'CreateDate'
                    if date_sort_col in final_df.columns:
                        sort_cols.append(date_sort_col)
                        ascending.append(False)

                final_df = final_df.sort_values(by=sort_cols, ascending=ascending)
            if action == 'show_summary':
                show_summary = True
                show_results = False
                creator_col = 'Creator' if 'Creator' in final_df.columns else ('rs_username' if 'rs_username' in final_df.columns else None)
                if not final_df.empty and creator_col and 'Package' in final_df.columns:
                    final_df['Package'] = pd.to_numeric(final_df['Package'], errors='coerce')
                    summary = (
                        final_df.groupby([creator_col, 'ServiceName'])
                        .agg(Count=('ServiceName', 'size'), SumGB=('Package', 'sum'))
                        .reset_index()
                    )
                    creator_rows = []
                    for creator in summary[creator_col].unique():
                        df_c = summary[summary[creator_col] == creator]
                        total_gb = df_c['SumGB'].astype(float).sum()
                        total_count = int(df_c['Count'].sum())
                        details = [
                            {
                                'ServiceName': row['ServiceName'],
                                'Count': int(row['Count']),
                                'SumGB': round(float(row['SumGB']), 2)
                            }
                            for _, row in df_c.iterrows()
                        ]
                        creator_rows.append({
                            'Creator': creator,
                            'TotalGB': round(total_gb, 2),
                            'TotalCount': total_count,
                            'Details': details
                        })
                    summary_rows = creator_rows
                    summary_grand_total = round(summary['SumGB'].astype(float).sum(), 2)
                    summary_grand_count = int(summary['Count'].sum())

            if action == 'download_summary_pdf':
                show_summary = True
                show_results = False
                creator_col = 'Creator' if 'Creator' in final_df.columns else ('rs_username' if 'rs_username' in final_df.columns else None)
                if not final_df.empty and creator_col and 'Package' in final_df.columns:
                    final_df['Package'] = pd.to_numeric(final_df['Package'], errors='coerce')
                    summary = (
                        final_df.groupby([creator_col, 'ServiceName'])
                        .agg(Count=('ServiceName', 'size'), SumGB=('Package', 'sum'))
                        .reset_index()
                    )
                    creator_rows = []
                    for creator in summary[creator_col].unique():
                        df_c = summary[summary[creator_col] == creator]
                        total_gb = df_c['SumGB'].astype(float).sum()
                        total_count = int(df_c['Count'].sum())
                        details = [
                            {
                                'ServiceName': row['ServiceName'],
                                'Count': int(row['Count']),
                                'SumGB': round(float(row['SumGB']), 2)
                            }
                            for _, row in df_c.iterrows()
                        ]
                        creator_rows.append({
                            'Creator': creator,
                            'TotalGB': round(total_gb, 2),
                            'TotalCount': total_count,
                            'Details': details
                        })
                    summary_rows = creator_rows
                    summary_grand_total = round(summary['SumGB'].astype(float).sum(), 2)
                    summary_grand_count = int(summary['Count'].sum())

                if summary_rows:
                    pdf_rows = []
                    for row in summary_rows:
                        for item in row['Details']:
                            pdf_rows.append({
                                'Creator': row['Creator'],
                                'ServiceName': item['ServiceName'],
                                'SumGB': item['SumGB'],
                                'Count': item['Count']
                            })
                        pdf_rows.append({
                            'Creator': f"{row['Creator']} Total",
                            'ServiceName': '',
                            'SumGB': row['TotalGB'],
                            'Count': row['TotalCount']
                        })
                    pdf_rows.append({
                        'Creator': 'Grand Total',
                        'ServiceName': '',
                        'SumGB': summary_grand_total,
                        'Count': summary_grand_count
                    })
                    pdf_df = pd.DataFrame(pdf_rows)
                    pdf_data = export_df_to_pdf(pdf_df)
                    if pdf_data:
                        resp = HttpResponse(pdf_data, content_type='application/pdf')
                        filename = _build_report_filename('pdf', creators, effective_filters).replace('report-', 'summary-')
                        resp['Content-Disposition'] = f'attachment; filename="{filename}"'
                        return resp

            if action == 'download_report':
                pdf_df = final_df.copy()
                creator_col = 'Creator' if 'Creator' in pdf_df.columns else ('rs_username' if 'rs_username' in pdf_df.columns else None)
                if not pdf_df.empty and creator_col and ('Package' in pdf_df.columns or 'PackageValue' in pdf_df.columns):
                    pkg_col = 'Package' if 'Package' in pdf_df.columns else 'PackageValue'
                    if 'Count' not in pdf_df.columns:
                        pdf_df['Count'] = None
                    pkg_numeric = pd.to_numeric(pdf_df[pkg_col], errors='coerce')
                    totals = (
                        pdf_df.assign(_pkg=pkg_numeric)
                        .groupby(creator_col)
                        .agg(SumPkg=('_pkg', 'sum'), Count=('Count', 'size'))
                        .reset_index()
                    )

                    def _blank_row():
                        return {c: '' for c in pdf_df.columns}

                    total_rows = []
                    for _, row in totals.iterrows():
                        r = _blank_row()
                        r[creator_col] = f"{row[creator_col]} Total"
                        r[pkg_col] = round(float(row['SumPkg']) if pd.notna(row['SumPkg']) else 0, 2)
                        r['Count'] = int(row['Count'])
                        total_rows.append(r)

                    grand_total = float(pkg_numeric.sum()) if pkg_numeric.notna().any() else 0
                    grand_count = int(len(pdf_df))
                    r = _blank_row()
                    r[creator_col] = 'Grand Total'
                    r[pkg_col] = round(grand_total, 2)
                    r['Count'] = grand_count
                    total_rows.append(r)

                    pdf_df = pd.concat([pdf_df, pd.DataFrame(total_rows)], ignore_index=True)

                pdf_data = export_df_to_pdf(pdf_df)
                if pdf_data:
                    resp = HttpResponse(pdf_data, content_type='application/pdf')
                    filename = _build_report_filename('pdf', creators, effective_filters)
                    resp['Content-Disposition'] = f'attachment; filename="{filename}"'
                    return resp
            elif action == 'download_csv':
                csv_df = final_df.copy()
                creator_col = 'Creator' if 'Creator' in csv_df.columns else ('rs_username' if 'rs_username' in csv_df.columns else None)
                if not csv_df.empty and creator_col and ('Package' in csv_df.columns or 'PackageValue' in csv_df.columns):
                    pkg_col = 'Package' if 'Package' in csv_df.columns else 'PackageValue'
                    if 'Count' not in csv_df.columns:
                        csv_df['Count'] = None
                    pkg_numeric = pd.to_numeric(csv_df[pkg_col], errors='coerce')
                    totals = (
                        csv_df.assign(_pkg=pkg_numeric)
                        .groupby(creator_col)
                        .agg(SumPkg=('_pkg', 'sum'), Count=('Count', 'size'))
                        .reset_index()
                    )

                    def _blank_row_csv():
                        return {c: '' for c in csv_df.columns}

                    total_rows = []
                    for _, row in totals.iterrows():
                        r = _blank_row_csv()
                        r[creator_col] = f"{row[creator_col]} Total"
                        r[pkg_col] = round(float(row['SumPkg']) if pd.notna(row['SumPkg']) else 0, 2)
                        r['Count'] = int(row['Count'])
                        total_rows.append(r)

                    grand_total = float(pkg_numeric.sum()) if pkg_numeric.notna().any() else 0
                    grand_count = int(len(csv_df))
                    r = _blank_row_csv()
                    r[creator_col] = 'Grand Total'
                    r[pkg_col] = round(grand_total, 2)
                    r['Count'] = grand_count
                    total_rows.append(r)

                    csv_df = pd.concat([csv_df, pd.DataFrame(total_rows)], ignore_index=True)

                csv_data = csv_df.to_csv(index=False)
                resp = HttpResponse(csv_data, content_type='text/csv')
                filename = _build_report_filename('csv', creators, effective_filters)
                resp['Content-Disposition'] = f'attachment; filename="{filename}"'
                return resp

    columns = list(final_df.columns) if not final_df.empty else None
    rows = final_df.values.tolist() if not final_df.empty else None

    return render(request, 'report/report.html', {
        'form': form,
        'columns': columns,
        'rows': rows,
        'info_tables': info_tables,
        'show_results': show_results,
        'show_summary': show_summary,
        'summary_rows': summary_rows,
        'summary_grand_total': summary_grand_total,
        'summary_grand_count': summary_grand_count,
        'error': request.session.pop('error', None)
    })
