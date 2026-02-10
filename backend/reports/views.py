import os
import io
import re
import datetime
import tempfile
import pandas as pd
import qrcode
from django.conf import settings
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.contrib.auth import logout
from django.contrib.auth import views as auth_views
from django.http import HttpResponse, HttpResponseForbidden, FileResponse
from django.core.management import call_command
from django.core.files.base import ContentFile
from django.shortcuts import redirect
from .forms import FilterForm, CreatePackageForm
from maria_cache.sync import sync_reference_tables
from maria_cache.models import Reseller as CacheReseller
from .db import (
    fetch_allowed_centers,
    fetch_allowed_services,
    fetch_allowed_statuses,
    fetch_general_permissions,
    fetch_reseller_by_username,
    fetch_supporters,
    fetch_visps_for_reseller,
    get_sources,
)
from .user_create import create_users, UserCreateError
from .models import ResellerProfile, PdfArchive
from fpdf import FPDF

from .db import run_query
from .bq import run_bq_report_query
from .sync import read_sync_logs, sync_maria_to_bigquery


def safe_text(text):
    try:
        return str(text).encode('latin-1', 'ignore').decode('latin-1')
    except Exception:
        return ''


_MOBILE_UA_RE = re.compile(r"android|iphone|ipad|ipod|mobile|iemobile|blackberry|opera mini", re.I)


def _is_mobile_request(request):
    user_agent = request.META.get('HTTP_USER_AGENT', '')
    return bool(_MOBILE_UA_RE.search(user_agent))


def _select_template(request, desktop_template, mobile_template):
    return mobile_template if _is_mobile_request(request) else desktop_template


def _truncate_text(pdf, text, width):
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


def export_qr_vouchers_pdf(created, selection, frame_path):
    if not created:
        return None
    if not os.path.exists(frame_path):
        return None

    pdf = PDF(orientation='P', unit='pt', format='A4')
    pdf.set_auto_page_break(auto=False)

    base_w, base_h = 600.0, 900.0
    outer_margin = 4
    gap = 2

    cols, rows = 4, 3
    cell_w = (pdf.w - (outer_margin * 2) - (gap * (cols - 1))) / cols
    cell_h = (pdf.h - (outer_margin * 2) - (gap * (rows - 1))) / rows

    qr_box = {'x': 60, 'y': 80, 'w': 480, 'h': 440}
    qr_margin = 4
    cred_box = {'x': 60, 'y': 540, 'w': 480, 'h': 90}
    detail_box = {'x': 50, 'y': 650, 'w': 500, 'h': 130}

    safe_service = safe_text(selection.get('service') or '')

    def _fit_font_size(text, max_w, max_size, min_size, weight='B'):
        size = max_size
        pdf.set_font('Helvetica', weight, size)
        while size > min_size and pdf.get_string_width(text) > max_w:
            size -= 0.5
            pdf.set_font('Helvetica', weight, size)
        return size

    for index, row in enumerate(created):
        username = safe_text(row.get('username') or '')
        password = safe_text(row.get('password') or '')
        qr_payload = f"username:{username}\npassword:{password}"

        if index % (cols * rows) == 0:
            pdf.add_page()

        slot = index % (cols * rows)
        row_idx = slot // cols
        col_idx = slot % cols
        cell_x = outer_margin + (col_idx * (cell_w + gap))
        cell_y = outer_margin + (row_idx * (cell_h + gap))

        scale = min(cell_w / base_w, cell_h / base_h)
        frame_w = base_w * scale
        frame_h = base_h * scale
        frame_x = cell_x + (cell_w - frame_w) / 2
        frame_y = cell_y + (cell_h - frame_h) / 2

        pdf.image(frame_path, x=frame_x, y=frame_y, w=frame_w, h=frame_h)

        qr_img = qrcode.make(qr_payload)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        try:
            qr_img.save(tmp.name)
            qr_w = max(0, (qr_box['w'] - (qr_margin * 2)) * scale)
            qr_h = max(0, (qr_box['h'] - (qr_margin * 2)) * scale)
            qr_size = min(qr_w, qr_h)
            qr_x = frame_x + (qr_box['x'] * scale) + ((qr_box['w'] * scale) - qr_size) / 2
            qr_y = frame_y + (qr_box['y'] * scale) + ((qr_box['h'] * scale) - qr_size) / 2
            pdf.image(tmp.name, x=qr_x, y=qr_y, w=qr_size, h=qr_size)
        finally:
            tmp.close()
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

        icon_center_x = frame_x + (108 * scale)
        icon_center_y = frame_y + (590 * scale)
        icon_w = 70 * scale
        icon_x = icon_center_x - (icon_w / 2)

        pdf.set_text_color(0, 0, 0)
        text_x = icon_x + icon_w + (8 * scale)
        right_edge = frame_x + ((cred_box['x'] + cred_box['w']) * scale) - (20 * scale)
        text_w = max(0, right_edge - text_x)
        text_line = f"username: {username}   password: {password}"
        max_size = 16 * scale
        min_size = 8 * scale
        size = _fit_font_size(text_line, text_w, max_size, min_size, weight='B')
        line_h = size * 1.15
        text_y = icon_center_y - (line_h / 2)
        pdf.set_xy(text_x, text_y)
        pdf.set_font('Helvetica', 'B', size)
        pdf.cell(text_w, line_h, text_line)

        pdf.set_text_color(255, 255, 255)
        detail_x = frame_x + (detail_box['x'] * scale) + (20 * scale)
        detail_w = (detail_box['w'] * scale) - (40 * scale)
        package_text = f"Package: {safe_service}"
        max_size = 20 * scale
        min_size = 8 * scale
        size = _fit_font_size(package_text, detail_w, max_size, min_size, weight='B')
        line_h = size * 1.15
        detail_y = frame_y + (detail_box['y'] * scale) + ((detail_box['h'] * scale) - line_h) * 0.3
        pdf.set_xy(detail_x, detail_y)
        pdf.cell(detail_w, line_h, package_text, align='C')

    return pdf.output(dest='S').encode('latin1')


def logout_view(request):
    if request.method not in {'GET', 'POST'}:
        return HttpResponseForbidden()
    logout(request)
    return redirect('reports:login')


def _summary_rows_to_df(rows, grand_total, grand_count):
    if not rows:
        return pd.DataFrame(columns=['Creator', 'ServiceName', 'SumGB', 'Count'])

    pdf_rows = []
    for row in rows:
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
        'SumGB': grand_total,
        'Count': grand_count
    })
    return pd.DataFrame(pdf_rows)


def export_summary_tables_to_pdf(limited_df, unlimited_df):
    pdf = PDF(orientation='L')
    pdf.set_auto_page_break(auto=True, margin=8)
    pdf.add_page()
    try:
        pdf.set_font("Arial", size=8)
    except Exception:
        pdf.set_font("helvetica", size=8)

    line_height = 6
    min_width = 18
    max_width = 80
    left_x = pdf.l_margin
    table_width = pdf.w - pdf.l_margin - pdf.r_margin

    def _safe_str(value):
        if value is None:
            return ""
        return safe_text(value)

    def _calc_widths(df, cols):
        col_widths = []
        for c in cols:
            values = df[c].astype(str).fillna('') if c in df.columns else pd.Series([], dtype=str)
            sample = list(values.head(200))
            max_text = max([c] + sample, key=lambda s: pdf.get_string_width(str(s)))
            w = pdf.get_string_width(str(max_text)) + 6
            col_widths.append(max(min_width, min(max_width, w)))
        total = sum(col_widths)
        if total > table_width:
            scale = table_width / total
            col_widths = [max(min_width, w * scale) for w in col_widths]
        return col_widths

    def _render_table(df, title, start_y):
        pdf.set_xy(left_x, start_y)
        pdf.set_font(pdf.font_family, size=9)
        pdf.cell(table_width, line_height, _safe_str(title), border=0)
        pdf.ln(line_height + 1)

        if df.empty:
            pdf.set_x(left_x)
            pdf.cell(table_width, line_height, "No data", border=1, align='C')
            pdf.ln(line_height + 2)
            return pdf.get_y()

        cols = ['Creator', 'ServiceName', 'SumGB', 'Count']
        col_widths = _calc_widths(df, cols)
        pdf.set_fill_color(255, 255, 255)
        for i, c in enumerate(cols):
            header_text = _truncate_text(pdf, _safe_str(c), col_widths[i])
            pdf.cell(col_widths[i], line_height, header_text, border=1, align='C', fill=True)
        pdf.ln(line_height)

        for _, row in df.iterrows():
            for i, c in enumerate(cols):
                cell_text = _truncate_text(pdf, _safe_str(row.get(c, '')), col_widths[i])
                pdf.cell(col_widths[i], line_height, cell_text, border=1)
            pdf.ln(line_height)

        pdf.ln(4)
        return pdf.get_y()

    y = pdf.t_margin
    y = _render_table(limited_df, 'Limited Packages Summary', y)
    _render_table(unlimited_df, 'Unlimited Packages Summary', y)

    return pdf.output(dest='S').encode('latin1')


def export_detail_tables_to_pdf(limited_df, unlimited_df):
    pdf = PDF(orientation='L')
    pdf.set_auto_page_break(auto=True, margin=8)
    pdf.add_page()
    try:
        pdf.set_font("Arial", size=8)
    except Exception:
        pdf.set_font("helvetica", size=8)

    line_height = 6
    min_width = 18
    max_width = 80
    left_x = pdf.l_margin
    table_width = pdf.w - pdf.l_margin - pdf.r_margin

    def _safe_str(value):
        if value is None:
            return ""
        return safe_text(value)

    def _calc_widths(df, cols):
        col_widths = []
        for c in cols:
            values = df[c].astype(str).fillna('') if c in df.columns else pd.Series([], dtype=str)
            sample = list(values.head(200))
            max_text = max([c] + sample, key=lambda s: pdf.get_string_width(str(s)))
            w = pdf.get_string_width(str(max_text)) + 6
            col_widths.append(max(min_width, min(max_width, w)))
        total = sum(col_widths)
        if total > table_width:
            scale = table_width / total
            col_widths = [max(min_width, w * scale) for w in col_widths]
        return col_widths

    def _render_table(df, title, start_y):
        pdf.set_xy(left_x, start_y)
        pdf.set_font(pdf.font_family, size=9)
        pdf.cell(table_width, line_height, _safe_str(title), border=0)
        pdf.ln(line_height + 1)

        if df.empty:
            pdf.set_x(left_x)
            pdf.cell(table_width, line_height, "No data", border=1, align='C')
            pdf.ln(line_height + 2)
            return pdf.get_y()

        cols = list(df.columns)
        col_widths = _calc_widths(df, cols)
        pdf.set_fill_color(255, 255, 255)
        for i, c in enumerate(cols):
            header_text = _truncate_text(pdf, _safe_str(c), col_widths[i])
            pdf.cell(col_widths[i], line_height, header_text, border=1, align='C', fill=True)
        pdf.ln(line_height)

        for _, row in df.iterrows():
            for i, c in enumerate(cols):
                cell_text = _truncate_text(pdf, _safe_str(row.get(c, '')), col_widths[i])
                pdf.cell(col_widths[i], line_height, cell_text, border=1)
            pdf.ln(line_height)

        pdf.ln(4)
        return pdf.get_y()

    y = pdf.t_margin
    y = _render_table(limited_df, 'Limited Packages Report', y)
    _render_table(unlimited_df, 'Unlimited Packages Report', y)

    return pdf.output(dest='S').encode('latin1')


@login_required
def create_package_view(request):
    template_name = _select_template(request, 'report/create_package.html', 'report/create_package_mobile.html')
    error = None
    info = None
    selection = None
    creation_log = None
    reseller_valid = False
    reseller_id = None

    def _safe_choices(choices, empty_label):
        return choices or [('', empty_label)]

    sources = get_sources()
    server_choices = [(s.get('name'), s.get('name')) for s in sources if s.get('name')]
    selected_server = request.POST.get('server_name') or (server_choices[0][0] if server_choices else None)

    if request.method == 'POST' and request.POST.get('action') in {'sync_cache', 'sync_all'}:
        action = request.POST.get('action')
        if action == 'sync_all':
            sources_list = [s.get('name') for s in sources if s.get('name')]
            if not sources_list:
                error = 'No MariaDB servers available. Check MARIA_SOURCES.'
            else:
                failed = []
                for source_name in sources_list:
                    try:
                        sync_reference_tables(source_name=source_name)
                    except Exception as exc:
                        failed.append(f"{source_name}: {exc}")
                if failed:
                    error = 'Sync failed for: ' + '; '.join(failed)
                else:
                    info = 'Sync completed for all servers.'
        else:
            if not selected_server:
                error = 'No MariaDB server selected.'
            else:
                try:
                    sync_reference_tables(source_name=selected_server)
                    info = f"Sync completed for {selected_server}."
                except Exception as exc:
                    error = f"Sync failed: {exc}"

    try:
        hidden_service_ids = {
            32, 126, 78, 110, 129, 105, 69, 84, 137, 8, 7, 9, 10, 117, 132, 91, 73, 143, 71, 70, 72, 76, 75
        }
        default_supporter_id = None
        default_status_id = None
        choices_map = {
            'servers': server_choices,
            'resellers': [],
            'visps': [],
            'centers': [],
            'supporters': [],
            'statuses': [],
            'services': [],
        }

        if selected_server:
            choices_map['supporters'] = fetch_supporters(source_name=selected_server)
            for supporter in choices_map['supporters']:
                if supporter and str(supporter[1]).strip().lower() == 'default-supporter':
                    default_supporter_id = supporter[0]
                    break
            if default_supporter_id is None and choices_map['supporters']:
                default_supporter_id = choices_map['supporters'][0][0]
            if default_supporter_id is not None:
                choices_map['default_supporter_id'] = default_supporter_id

            reseller_username = (request.POST.get('reseller_username') or '').strip()
            action = request.POST.get('action')
            if reseller_username and action in {'check_reseller', 'create'}:
                if not CacheReseller.objects.using('cache').filter(source_name=selected_server).exists():
                    error = error or 'Permission cache is empty. Click "همگام سازی تمام سرور ها" first.'
                    reseller = None
                else:
                    reseller = fetch_reseller_by_username(reseller_username, source_name=selected_server)
                if reseller and reseller.get('id') is not None:
                    reseller_valid = True
                    reseller_id = reseller['id']

                    visps = fetch_visps_for_reseller(reseller_id, source_name=selected_server)
                    visp_ids = [int(v[0]) for v in visps if v and v[0] and int(v[0]) > 0]

                    services = fetch_allowed_services(reseller_id, visp_ids, source_name=selected_server)
                    services = [s for s in services if s and s[0] and int(s[0]) not in hidden_service_ids]
                    service_ids = [int(s[0]) for s in services if s and s[0]]

                    statuses = fetch_allowed_statuses(reseller_id, visp_ids, source_name=selected_server)
                    centers = fetch_allowed_centers(visp_ids, source_name=selected_server)

                    for status in statuses:
                        if status and str(status[1]).strip().lower() == 'active':
                            default_status_id = status[0]
                            break
                    if default_status_id is None and statuses:
                        default_status_id = statuses[0][0]

                    choices_map['visps'] = _safe_choices(visps, 'No VISP permissions found')
                    choices_map['services'] = _safe_choices(services, 'No services permitted')
                    choices_map['statuses'] = _safe_choices(statuses, 'No statuses permitted')
                    choices_map['centers'] = _safe_choices(centers, 'No centers permitted')
                    if default_status_id is not None:
                        choices_map['default_status_id'] = default_status_id

                    general_permissions = fetch_general_permissions(reseller_id, source_name=selected_server)
                    if action == 'check_reseller':
                        info = f"Reseller verified for {selected_server}."
                        if general_permissions:
                            info += f" General permits: {', '.join(general_permissions)}"
                else:
                    error = error or 'Reseller username not found for selected server.'
        else:
            error = error or 'No MariaDB server available. Check MARIA_SOURCES.'
    except Exception as exc:
        choices_map = {
            'servers': server_choices,
            'resellers': [],
            'visps': [],
            'centers': [],
            'supporters': [],
            'statuses': [],
            'services': [],
        }
        error = error or f"Local cache lookup failed: {exc}"

    if request.method == 'POST' and (default_supporter_id or default_status_id):
        post_data = request.POST.copy()
        if default_supporter_id and not post_data.get('supporter_id'):
            post_data['supporter_id'] = default_supporter_id
        if default_status_id and not post_data.get('status_id'):
            post_data['status_id'] = default_status_id
        form = CreatePackageForm(post_data, choices_map=choices_map)
    else:
        form = CreatePackageForm(request.POST or None, choices_map=choices_map)

    if request.method == 'POST' and request.POST.get('action') == 'create':
        if form.is_valid():
            if not reseller_valid or reseller_id is None:
                error = 'Reseller username is not valid for the selected server.'
            else:
                reseller_label = (request.POST.get('reseller_username') or '').strip()
                visp_label = dict(choices_map.get('visps', [])).get(form.cleaned_data['visp_id'])
                center_label = dict(choices_map.get('centers', [])).get(form.cleaned_data['center_id'])
                supporter_label = dict(choices_map.get('supporters', [])).get(form.cleaned_data['supporter_id'])
                status_label = dict(choices_map.get('statuses', [])).get(form.cleaned_data['status_id'])
                service_label = dict(choices_map.get('services', [])).get(form.cleaned_data['service_id'])

                selection = {
                    'server_name': selected_server,
                    'reseller_username': (request.POST.get('reseller_username') or '').strip(),
                    'user_count': form.cleaned_data['user_count'],
                    'username_prefix': form.cleaned_data['username_prefix'],
                    'service': service_label or form.cleaned_data['service_id'],
                    'reseller': reseller_label or reseller_id,
                    'visp': visp_label or form.cleaned_data['visp_id'],
                    'center': center_label or form.cleaned_data['center_id'],
                    'supporter': supporter_label or form.cleaned_data['supporter_id'],
                    'status': status_label or form.cleaned_data['status_id'],
                }

                try:
                    payload = dict(form.cleaned_data)
                    payload['server_name'] = selected_server
                    payload['reseller_id'] = reseller_id
                    result = create_users(payload)
                    creation_log = result
                    selection['created_date'] = datetime.datetime.now().strftime('%Y-%m-%d')
                    request.session['created_users'] = {
                        'batch_id': result.get('batch_id'),
                        'batch_name': result.get('batch_name'),
                        'created': result.get('created', []),
                        'selection': selection,
                    }
                    info = f"Created {len(result.get('created', []))} users. Batch #{result.get('batch_id')}"
                except UserCreateError as exc:
                    error = str(exc)
                except Exception as exc:
                    error = f"Create failed: {exc}"
        else:
            errors = []
            for field, field_errors in form.errors.items():
                for field_error in field_errors:
                    errors.append(f"{field}: {field_error}")
            error = 'Invalid input. ' + '; '.join(errors) if errors else 'Invalid input.'

    pdf_archives = PdfArchive.objects.none()
    if request.user.is_authenticated:
        pdf_archives = PdfArchive.objects.filter(created_by=request.user).order_by('-created_at')[:10]

    return render(request, template_name, {
        'form': form,
        'error': error,
        'info': info,
        'selection': selection,
        'creation_log': creation_log,
        'reseller_valid': reseller_valid,
        'pdf_archives': pdf_archives,
        'created_pdf_ids': (request.session.get('created_users') or {}).get('pdf_ids', {}),
    })


def _save_pdf_archive(request, pdf_bytes, pdf_type, data, selection):
    record = PdfArchive.objects.create(
        pdf_type=pdf_type,
        created_by=request.user if request.user.is_authenticated else None,
        batch_id=data.get('batch_id'),
        batch_name=data.get('batch_name') or '',
        reseller_username=selection.get('reseller_username') or '',
        service_name=selection.get('service') or '',
        user_count=selection.get('user_count') or None,
    )
    record.file.save(f"{record.id}.pdf", ContentFile(pdf_bytes), save=True)
    return record


@login_required
def download_created_users_pdf(request):
    data = request.session.get('created_users') or {}
    created = data.get('created') or []
    selection = data.get('selection') or {}
    if not created:
        return HttpResponse('No created users to download.', content_type='text/plain')

    pdf_ids = data.get('pdf_ids') or {}
    existing_id = pdf_ids.get('details')
    if existing_id:
        record = PdfArchive.objects.filter(
            id=existing_id,
            pdf_type=PdfArchive.TYPE_DETAILS,
            created_by=request.user,
        ).first()
        if record and record.file:
            return FileResponse(record.file.open('rb'), as_attachment=True, filename=f"{record.id}.pdf")

    df = pd.DataFrame(created)
    pdf_bytes = export_df_to_pdf(df)
    if not pdf_bytes:
        return HttpResponse('Failed to generate PDF.', content_type='text/plain')

    record = _save_pdf_archive(request, pdf_bytes, PdfArchive.TYPE_DETAILS, data, selection)
    data['pdf_ids'] = {**pdf_ids, 'details': record.id}
    request.session['created_users'] = data
    return FileResponse(record.file.open('rb'), as_attachment=True, filename=f"{record.id}.pdf")


@login_required
def download_created_users_qr_pdf(request):
    data = request.session.get('created_users') or {}
    created = data.get('created') or []
    selection = data.get('selection') or {}
    if not created:
        return HttpResponse('No created users to download.', content_type='text/plain')

    pdf_ids = data.get('pdf_ids') or {}
    existing_id = pdf_ids.get('qr')
    if existing_id:
        record = PdfArchive.objects.filter(
            id=existing_id,
            pdf_type=PdfArchive.TYPE_QR,
            created_by=request.user,
        ).first()
        if record and record.file:
            return FileResponse(record.file.open('rb'), as_attachment=True, filename=f"{record.id}.pdf")

    frame_path = os.path.join(settings.BASE_DIR, 'assets', 'frame.png')
    pdf_bytes = export_qr_vouchers_pdf(created, selection, frame_path)
    if not pdf_bytes:
        return HttpResponse('Failed to generate QR PDF.', content_type='text/plain')

    record = _save_pdf_archive(request, pdf_bytes, PdfArchive.TYPE_QR, data, selection)
    data['pdf_ids'] = {**pdf_ids, 'qr': record.id}
    request.session['created_users'] = data
    return FileResponse(record.file.open('rb'), as_attachment=True, filename=f"{record.id}.pdf")


@login_required
def download_pdf_archive(request):
    pdf_id = (request.GET.get('pdf_id') or '').strip()
    if not pdf_id.isdigit():
        return HttpResponse('Invalid PDF id.', content_type='text/plain')

    record = PdfArchive.objects.filter(id=int(pdf_id), created_by=request.user).first()
    if not record or not record.file:
        return HttpResponse('PDF not found.', content_type='text/plain')

    return FileResponse(record.file.open('rb'), as_attachment=True, filename=f"{record.id}.pdf")


@login_required
def manual_sync_permissions(request):
    if request.method != 'POST':
        return HttpResponseForbidden()
    try:
        call_command('sync_permissions_cache')
        return redirect('reports:create_package')
    except Exception as exc:
        return HttpResponse(f"Sync failed: {exc}", content_type='text/plain')


@login_required
def report_view(request):
    template_name = _select_template(request, 'report/report.html', 'report/report_mobile.html')
    if request.method == 'GET':
        request.session.pop('report_filters', None)
    form = FilterForm(request.POST or None)
    final_df = pd.DataFrame()
    info_tables = []
    show_results = False
    show_summary = False
    summary_rows = []
    summary_grand_total = None
    summary_grand_count = None
    unlimited_summary_rows = []
    unlimited_grand_total = None
    unlimited_grand_count = None

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
            return render(request, template_name, {
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
            if date_op in {'EXACT', '=', '>', '<', '>=', '<='} and date_value:
                op_map = {'EXACT': 'eq', '=': 'eq', '>': 'gt', '<': 'lt', '>=': 'gte', '<=': 'lte'}
                parts.append(f"date-{op_map.get(date_op, 'eq')}-{_safe_filename(date_value)}")
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
            elif serial_op in {'=', '>', '<', '>=', '<='} and serial_value is not None:
                op_map = {'=': 'eq', '>': 'gt', '<': 'lt', '>=': 'gte', '<=': 'lte'}
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

        def _build_summary_rows(source_df, creator_col):
            if source_df.empty or not creator_col or 'ServiceName' not in source_df.columns:
                return [], None, None

            df = source_df.copy()
            if 'Package' not in df.columns and 'PackageValue' in df.columns:
                df['Package'] = df['PackageValue']
            if 'Package' not in df.columns:
                return [], None, None

            df['Package'] = pd.to_numeric(df['Package'], errors='coerce')
            summary = (
                df.groupby([creator_col, 'ServiceName'])
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

            grand_total = round(summary['SumGB'].astype(float).sum(), 2) if not summary.empty else None
            grand_count = int(summary['Count'].sum()) if not summary.empty else None
            return creator_rows, grand_total, grand_count

        def _unlimited_mask(source_df):
            if source_df.empty:
                return pd.Series([], dtype=bool)

            if 'PackageBytes' in source_df.columns:
                series = pd.to_numeric(source_df['PackageBytes'], errors='coerce')
            elif 'Package' in source_df.columns:
                series = pd.to_numeric(source_df['Package'], errors='coerce')
            elif 'PackageValue' in source_df.columns:
                series = pd.to_numeric(source_df['PackageValue'], errors='coerce')
            else:
                return pd.Series([False] * len(source_df), index=source_df.index)

            base_unlimited = series.isna() | (series <= 0)

            if 'ServiceName' not in source_df.columns:
                return base_unlimited

            name_series = source_df['ServiceName'].fillna('').astype(str)
            gb_pattern = re.compile(r'(?:\d+(?:\.\d+)?)[\s_-]*(?:gb|gig)\b', re.IGNORECASE)
            name_has_gb = name_series.str.contains(gb_pattern)
            name_has_ddc = name_series.str.contains(r'\bDDC\b', case=False, regex=True)

            # Unlimited only when GB is missing/zero and name matches unlimited rules.
            return base_unlimited & ((~name_has_gb) | name_has_ddc)

        if use_bq:
            try:
                bq_date_op = form.cleaned_data.get('date_op')
                bq_date_value = form.cleaned_data.get('date_value')
                bq_date_start = form.cleaned_data.get('date_start')
                bq_date_end = form.cleaned_data.get('date_end')
                bq_service_status = form.cleaned_data.get('service_status')

                if bq_date_op == 'NONE':
                    bq_date_op = None
                if bq_date_op in (None, '') and bq_date_start and bq_date_end:
                    bq_date_op = 'BETWEEN'
                if bq_date_op in {'EXACT', '='} and not bq_date_value and bq_date_start and bq_date_end:
                    bq_date_op = 'BETWEEN'
                if bq_date_op in {'<', '>', '<=', '>='} and not bq_date_value and bq_date_start and bq_date_end:
                    bq_date_op = 'BETWEEN'
                if bq_date_op == 'BETWEEN' and (not bq_date_start or not bq_date_end) and bq_date_value:
                    bq_date_op = '='
                df, used_table = run_bq_report_query(
                    creators,
                    limit=limit,
                    date_op=bq_date_op,
                    date_value=bq_date_value,
                    date_start=bq_date_start,
                    date_end=bq_date_end,
                    service_status=bq_service_status,
                )
                if not df.empty:
                    total_dfs.append(df)
                    creators_label = ', '.join([c for c in creators if c]) if creators else 'all'
                    info_tables.append(f"{creators_label} ← {used_table}")
            except Exception as exc:
                request.session['error'] = str(exc)
        else:
            service_status = form.cleaned_data.get('service_status')
            status_filter_sql = ''
            status_params = []
            if service_status and service_status not in {'NONE', ''}:
                status_filter_sql = "\n  AND TName.ServiceStatus = %s"
                status_params = [service_status]
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
    COALESCE(NULLIF(Hse.STrA, 0), NULLIF(Hse.MTrA, 0), NULLIF(Hse.DTrA, 0), NULLIF(Hse.YTrA, 0), NULLIF(Hse.ExtraTraffic, 0)) AS PackageBytes,
    CASE
        WHEN COALESCE(NULLIF(Hse.STrA, 0), NULLIF(Hse.MTrA, 0), NULLIF(Hse.DTrA, 0), NULLIF(Hse.YTrA, 0), NULLIF(Hse.ExtraTraffic, 0)) IS NULL THEN NULL
        ELSE ROUND(COALESCE(NULLIF(Hse.STrA, 0), NULLIF(Hse.MTrA, 0), NULLIF(Hse.DTrA, 0), NULLIF(Hse.YTrA, 0), NULLIF(Hse.ExtraTraffic, 0)) / 1073741824, 2)
    END AS PackageValue
FROM {table_path} TName
JOIN Huser Hu ON TName.User_Id = Hu.User_Id
LEFT JOIN Hreseller Hrc ON TName.Creator_Id = Hrc.Reseller_Id
LEFT JOIN Hservice Hse ON TName.Service_Id = Hse.Service_Id
WHERE (TRIM(LOWER(Hrc.ResellerName)) = TRIM(LOWER(%s)) OR %s IS NULL)
"""
                if status_filter_sql:
                    query_base += status_filter_sql
                query_base += """
ORDER BY TName.CDT DESC
LIMIT %s
"""
                params = [creator, creator]
                params.extend(status_params)
                params.append(limit)
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
                if pkg_numeric.notna().any():
                    final_df['Package'] = pkg_numeric.round(2)

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
                or (form.cleaned_data.get('service_status') not in (None, '', 'NONE'))
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
                    'service_status': form.cleaned_data.get('service_status'),
                }
            else:
                if creators_raw:
                    request.session.pop('report_filters', None)

            session_filters = request.session.get('report_filters') or {}
            use_session_filters = (
                not filter_input_present
                and action in {'download_report', 'download_csv', 'download_summary_pdf', 'download_unlimited_pdf'}
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
                    elif serial_op == '>=' and serial_value is not None:
                        final_df = final_df[serial_series >= serial_value]
                    elif serial_op == '<=' and serial_value is not None:
                        final_df = final_df[serial_series <= serial_value]

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
                        date_op = '='
                    if date_op == 'EXACT':
                        date_op = '='
                    if date_op == '=' and date_value is None and date_start and date_end:
                        date_op = 'BETWEEN'
                    elif date_op == 'BETWEEN' and (not date_start or not date_end) and date_value:
                        date_op = '='

                    date_series = pd.to_datetime(final_df[date_col], errors='coerce')
                    if date_op == '=' and date_value:
                        final_df = final_df[date_series.dt.date == date_value]
                    elif date_op == 'BETWEEN' and date_start and date_end:
                        final_df = final_df[(date_series.dt.date >= date_start) & (date_series.dt.date <= date_end)]
                    elif date_op == '>' and date_value:
                        final_df = final_df[date_series.dt.date > date_value]
                    elif date_op == '<' and date_value:
                        final_df = final_df[date_series.dt.date < date_value]
                    elif date_op == '>=' and date_value:
                        final_df = final_df[date_series.dt.date >= date_value]
                    elif date_op == '<=' and date_value:
                        final_df = final_df[date_series.dt.date <= date_value]

            # apply service status filter
            service_status = effective_filters.get('service_status')
            if service_status and service_status not in {'NONE', ''}:
                if 'ServiceStatus' in final_df.columns:
                    status_norm = str(service_status).strip().lower()
                    final_df = final_df[
                        final_df['ServiceStatus']
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .eq(status_norm)
                    ]

                    if status_norm == 'pending' and 'Username' in final_df.columns:
                        date_col = 'CreateDT' if 'CreateDT' in final_df.columns else 'CreateDate'
                        if date_col in final_df.columns:
                            final_df = final_df.copy()
                            final_df['_tmp_dt'] = pd.to_datetime(final_df[date_col], errors='coerce')
                            final_df = final_df.sort_values(by=['Username', '_tmp_dt'], ascending=[True, False])
                            final_df = final_df.drop_duplicates(subset=['Username'], keep='first')
                            final_df = final_df.drop(columns=['_tmp_dt'])

                        for col in ['password', 'Password', 'passwd', 'Passwd']:
                            if col in final_df.columns:
                                final_df = final_df.drop(columns=[col])

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
                elif sib_serial_op == '>=' and sib_serial_value is not None:
                    final_df = final_df[sib_series >= sib_serial_value]
                elif sib_serial_op == '<=' and sib_serial_value is not None:
                    final_df = final_df[sib_series <= sib_serial_value]

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
            if action in {'show_summary', 'download_summary_pdf', 'download_unlimited_pdf'}:
                show_summary = True
                show_results = False
                creator_col = 'Creator' if 'Creator' in final_df.columns else (
                    'rs_username' if 'rs_username' in final_df.columns else None
                )
                unlimited_mask = _unlimited_mask(final_df)
                unlimited_df = final_df[unlimited_mask]
                limited_df = final_df[~unlimited_mask]

                summary_rows, summary_grand_total, summary_grand_count = _build_summary_rows(limited_df, creator_col)
                unlimited_summary_rows, unlimited_grand_total, unlimited_grand_count = _build_summary_rows(
                    unlimited_df, creator_col
                )

                if action in {'download_summary_pdf', 'download_unlimited_pdf'}:
                    if action == 'download_unlimited_pdf':
                        limited_df = _summary_rows_to_df(summary_rows, summary_grand_total, summary_grand_count)
                        unlimited_df = _summary_rows_to_df(unlimited_summary_rows, unlimited_grand_total, unlimited_grand_count)
                        pdf_data = export_summary_tables_to_pdf(limited_df, unlimited_df)
                        if pdf_data:
                            resp = HttpResponse(pdf_data, content_type='application/pdf')
                            filename = _build_report_filename('pdf', creators, effective_filters).replace('report-', 'combined-summary-')
                            resp['Content-Disposition'] = f'attachment; filename="{filename}"'
                            return resp
                    else:
                        limited_df = _summary_rows_to_df(summary_rows, summary_grand_total, summary_grand_count)
                        unlimited_df = _summary_rows_to_df(unlimited_summary_rows, unlimited_grand_total, unlimited_grand_count)
                        pdf_data = export_summary_tables_to_pdf(limited_df, unlimited_df)
                        if pdf_data:
                            resp = HttpResponse(pdf_data, content_type='application/pdf')
                            filename = _build_report_filename('pdf', creators, effective_filters).replace('report-', 'summary-')
                            resp['Content-Disposition'] = f'attachment; filename="{filename}"'
                            return resp

            if action == 'download_report':
                pdf_df = final_df.copy()
                creator_col = 'Creator' if 'Creator' in pdf_df.columns else ('rs_username' if 'rs_username' in pdf_df.columns else None)

                def _add_totals(df):
                    if df.empty or not creator_col or ('Package' not in df.columns and 'PackageValue' not in df.columns):
                        return df

                    pkg_col = 'Package' if 'Package' in df.columns else 'PackageValue'
                    if 'Count' not in df.columns:
                        df['Count'] = None
                    pkg_numeric = pd.to_numeric(df[pkg_col], errors='coerce')
                    totals = (
                        df.assign(_pkg=pkg_numeric)
                        .groupby(creator_col)
                        .agg(SumPkg=('_pkg', 'sum'), Count=('Count', 'size'))
                        .reset_index()
                    )

                    def _blank_row():
                        return {c: '' for c in df.columns}

                    total_rows = []
                    for _, row in totals.iterrows():
                        r = _blank_row()
                        r[creator_col] = f"{row[creator_col]} Total"
                        r[pkg_col] = round(float(row['SumPkg']) if pd.notna(row['SumPkg']) else 0, 2)
                        r['Count'] = int(row['Count'])
                        total_rows.append(r)

                    grand_total = float(pkg_numeric.sum()) if pkg_numeric.notna().any() else 0
                    grand_count = int(len(df))
                    r = _blank_row()
                    r[creator_col] = 'Grand Total'
                    r[pkg_col] = round(grand_total, 2)
                    r['Count'] = grand_count
                    total_rows.append(r)

                    return pd.concat([df, pd.DataFrame(total_rows)], ignore_index=True)

                unlimited_mask = _unlimited_mask(pdf_df)
                limited_df = _add_totals(pdf_df[~unlimited_mask].copy())
                unlimited_df = _add_totals(pdf_df[unlimited_mask].copy())

                pdf_data = export_detail_tables_to_pdf(limited_df, unlimited_df)
                if pdf_data:
                    resp = HttpResponse(pdf_data, content_type='application/pdf')
                    filename = _build_report_filename('pdf', creators, effective_filters)
                    resp['Content-Disposition'] = f'attachment; filename="{filename}"'
                    return resp
            elif action == 'download_csv':
                csv_df = final_df.copy()
                creator_col = 'Creator' if 'Creator' in csv_df.columns else ('rs_username' if 'rs_username' in csv_df.columns else None)

                def _add_totals_csv(df):
                    if df.empty or not creator_col or ('Package' not in df.columns and 'PackageValue' not in df.columns):
                        return df

                    pkg_col = 'Package' if 'Package' in df.columns else 'PackageValue'
                    if 'Count' not in df.columns:
                        df['Count'] = None
                    pkg_numeric = pd.to_numeric(df[pkg_col], errors='coerce')
                    totals = (
                        df.assign(_pkg=pkg_numeric)
                        .groupby(creator_col)
                        .agg(SumPkg=('_pkg', 'sum'), Count=('Count', 'size'))
                        .reset_index()
                    )

                    def _blank_row_csv():
                        return {c: '' for c in df.columns}

                    total_rows = []
                    for _, row in totals.iterrows():
                        r = _blank_row_csv()
                        r[creator_col] = f"{row[creator_col]} Total"
                        r[pkg_col] = round(float(row['SumPkg']) if pd.notna(row['SumPkg']) else 0, 2)
                        r['Count'] = int(row['Count'])
                        total_rows.append(r)

                    grand_total = float(pkg_numeric.sum()) if pkg_numeric.notna().any() else 0
                    grand_count = int(len(df))
                    r = _blank_row_csv()
                    r[creator_col] = 'Grand Total'
                    r[pkg_col] = round(grand_total, 2)
                    r['Count'] = grand_count
                    total_rows.append(r)

                    return pd.concat([df, pd.DataFrame(total_rows)], ignore_index=True)

                unlimited_mask = _unlimited_mask(csv_df)
                limited_df = _add_totals_csv(csv_df[~unlimited_mask].copy())
                unlimited_df = _add_totals_csv(csv_df[unlimited_mask].copy())

                output = io.StringIO()
                output.write('Limited Packages Report\n')
                limited_df.to_csv(output, index=False)
                output.write('\n')
                output.write('Unlimited Packages Report\n')
                unlimited_df.to_csv(output, index=False)

                csv_data = output.getvalue()
                resp = HttpResponse(csv_data, content_type='text/csv')
                filename = _build_report_filename('csv', creators, effective_filters)
                resp['Content-Disposition'] = f'attachment; filename="{filename}"'
                return resp

    columns = list(final_df.columns) if not final_df.empty else None
    rows = final_df.values.tolist() if not final_df.empty else None

    return render(request, template_name, {
        'form': form,
        'columns': columns,
        'rows': rows,
        'info_tables': info_tables,
        'show_results': show_results,
        'show_summary': show_summary,
        'summary_rows': summary_rows,
        'summary_grand_total': summary_grand_total,
        'summary_grand_count': summary_grand_count,
        'unlimited_summary_rows': unlimited_summary_rows,
        'unlimited_grand_total': unlimited_grand_total,
        'unlimited_grand_count': unlimited_grand_count,
        'error': request.session.pop('error', None)
    })


@login_required
def sync_logs_view(request):
    if not request.user.is_superuser:
        return HttpResponseForbidden('Forbidden')

    run_result = None
    run_error = None
    limit_value = 0
    days_value = 0
    write_disposition = 'WRITE_TRUNCATE'

    if request.method == 'POST' and request.POST.get('action') == 'run_sync':
        try:
            limit_value = int(request.POST.get('limit') or 0)
        except ValueError:
            limit_value = 0
        try:
            days_value = int(request.POST.get('days') or 0)
        except ValueError:
            days_value = 0
        write_disposition = request.POST.get('write_disposition') or 'WRITE_TRUNCATE'
        try:
            days_param = days_value if days_value and days_value > 0 else None
            run_result = sync_maria_to_bigquery(
                limit=limit_value,
                write_disposition=write_disposition,
                days=days_param,
            )
        except Exception as exc:
            run_error = str(exc)

    logs = read_sync_logs(limit=200)
    auto_enabled = os.getenv('AUTO_SYNC_ENABLED', '0') == '1'
    auto_interval = int(os.getenv('AUTO_SYNC_INTERVAL_MINUTES', '30'))
    auto_days = int(os.getenv('AUTO_SYNC_DAYS', '90'))
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    next_run_at = now_utc + datetime.timedelta(minutes=auto_interval)

    auto_count = 0
    last_auto_entry = None
    for entry in logs:
        data = entry.get('data') or {}
        if data.get('auto') or entry.get('type') == 'auto_sync_start':
            auto_count += 1
    for entry in reversed(logs):
        data = entry.get('data') or {}
        if data.get('auto') or entry.get('type') == 'auto_sync_start':
            last_auto_entry = entry
            break

    template_name = _select_template(request, 'report/sync_logs.html', 'report/sync_logs_mobile.html')
    return render(request, template_name, {
        'logs': logs,
        'run_result': run_result,
        'run_error': run_error,
        'limit_value': limit_value,
        'days_value': days_value,
        'write_disposition': write_disposition,
        'auto_enabled': auto_enabled,
        'auto_interval': auto_interval,
        'auto_days': auto_days,
        'next_run_at': next_run_at,
        'auto_count': auto_count,
        'last_auto_entry': last_auto_entry,
    })


def login_view(request):
    template_name = _select_template(request, 'registration/login.html', 'registration/login_mobile.html')
    return auth_views.LoginView.as_view(template_name=template_name)(request)
