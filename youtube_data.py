import base64
import json
import logging
import os
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

import gspread
import isodate
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from get_urls import get_urls, normalize_channel_url

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler('youtube_data.log', mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


VIDEOS_CURRENT_SHEET = 'videos_current_60d'
VIDEOS_HISTORY_SHEET = 'videos_history'
CHANNEL_REGISTRY_SHEET = 'channel_registry'
HTTP_REDIRECT_TIMEOUT_SECONDS = 5
DEFAULT_LOOKBACK_DAYS = 60
RUN_REPORT_PATH = 'run_report.json'
UNRESOLVED_URLS_PATH = 'unresolved_urls.json'
CHANNEL_FAILURES_PATH = 'channel_failures.json'

CHANNEL_REGISTRY_COLUMNS = [
    'source_url',
    'normalized_url',
    'channel_id',
    'uploads_playlist_id',
    'channel_name',
    'resolver_type',
    'active',
    'resolution_status',
    'last_verified_at',
    'last_error',
]

EXPORT_COLUMNS = [
    'channel_name',
    'channel_id',
    'source_channel_url',
    'channel_custom_url',
    'channel_country',
    'channel_published_at',
    'subscriber_count_snapshot',
    'channel_total_views_snapshot',
    'channel_video_count_snapshot',
    'video_id',
    'video_url',
    'title',
    'description',
    'upload_date',
    'published_at_utc',
    'days_since_upload',
    'tags',
    'thumbnail_url',
    'duration_iso',
    'duration_seconds',
    'dimension',
    'definition',
    'caption',
    'licensed_content',
    'projection',
    'privacy_status',
    'license',
    'embeddable',
    'public_stats_viewable',
    'made_for_kids',
    'self_declared_made_for_kids',
    'category_id',
    'default_language',
    'default_audio_language',
    'live_broadcast_content',
    'views',
    'likes',
    'comments',
    'engagement_rate',
    'execution_date',
    'fetched_at',
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)



def utc_now_iso() -> str:
    return utc_now().strftime('%Y-%m-%dT%H:%M:%SZ')



def normalize_text(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, float):
        if pd.isna(value):
            return ''
        return format(value, 'f').rstrip('0').rstrip('.') or '0'
    if isinstance(value, int):
        return str(value)
    return str(value)



def normalize_bool_text(value: Any) -> str:
    normalized_value = normalize_text(value).strip().lower()
    return 'true' if normalized_value == 'true' else 'false'



def prepare_dataframe_with_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    prepared_df = df.copy()

    for column in columns:
        if column not in prepared_df.columns:
            prepared_df[column] = ''

    prepared_df = prepared_df[columns]
    prepared_df = prepared_df.fillna('')

    for column in prepared_df.columns:
        prepared_df[column] = prepared_df[column].map(normalize_text)

    return prepared_df



def parse_positive_int_env(var_name: str, default: int) -> int:
    raw_value = os.environ.get(var_name, '').strip()
    if not raw_value:
        return default
    parsed_value = int(raw_value)
    if parsed_value <= 0:
        raise ValueError(f'{var_name} debe ser mayor que 0.')
    return parsed_value



def parse_optional_positive_int_env(var_name: str) -> int | None:
    raw_value = os.environ.get(var_name, '').strip()
    if not raw_value:
        return None
    parsed_value = int(raw_value)
    if parsed_value <= 0:
        raise ValueError(f'{var_name} debe ser mayor que 0.')
    return parsed_value



def parse_bool_env(var_name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(var_name, '').strip().lower()
    if not raw_value:
        return default
    return raw_value in {'1', 'true', 'yes', 'y', 'on'}



def detect_execution_mode() -> str:
    return 'scheduled' if os.environ.get('GITHUB_EVENT_NAME', '').strip() == 'schedule' else 'manual'



def create_run_report(started_at: str) -> dict[str, Any]:
    return {
        'channels_total': 0,
        'channels_processed': 0,
        'channels_failed': 0,
        'rows_current_snapshot': 0,
        'registry_rows_total': 0,
        'registry_rows_active': 0,
        'registry_rows_failed': 0,
        'new_channels_resolved_this_run': 0,
        'videos_discovered': 0,
        'videos_exported': 0,
        'execution_mode': detect_execution_mode(),
        'days_window': DEFAULT_LOOKBACK_DAYS,
        'channel_limit_applied': '',
        'channel_filter_applied': '',
        'write_history_enabled': 'false',
        'history_rows_appended': 0,
        'api_calls': {
            'channels_list': 0,
            'playlistitems_list': 0,
            'videos_list': 0,
            'search_list': 0,
        },
        'unresolved_urls': [],
        'channel_failures': [],
        'started_at': started_at,
        'finished_at': '',
    }



def increment_api_call(run_report: dict[str, Any], api_name: str) -> None:
    run_report.setdefault('api_calls', {})
    run_report['api_calls'][api_name] = int(run_report['api_calls'].get(api_name, 0)) + 1



def write_json_file(data: Any, path: str) -> None:
    with open(path, 'w', encoding='utf-8') as file_handle:
        json.dump(data, file_handle, ensure_ascii=False, indent=2)



def write_run_report(run_report: dict[str, Any], path: str = RUN_REPORT_PATH) -> None:
    write_json_file(run_report, path)



def write_operational_artifacts(run_report: dict[str, Any]) -> None:
    write_run_report(run_report)
    write_json_file(run_report.get('unresolved_urls', []), UNRESOLVED_URLS_PATH)
    write_json_file(run_report.get('channel_failures', []), CHANNEL_FAILURES_PATH)



def iso_duration_to_seconds(duration: str) -> int:
    try:
        parsed_duration = isodate.parse_duration(duration)
        return int(parsed_duration.total_seconds())
    except Exception as exc:
        logger.error(f'Error al convertir la duración {duration}: {str(exc)}')
        logger.error(traceback.format_exc())
        return 0



def parse_upload_datetime(upload_date_str: str | None) -> datetime | None:
    if not upload_date_str:
        return None
    parsed_datetime = isodate.parse_datetime(upload_date_str)
    if parsed_datetime.tzinfo is None:
        parsed_datetime = parsed_datetime.replace(tzinfo=timezone.utc)
    return parsed_datetime.astimezone(timezone.utc)



def fetch_channel_context(
    youtube: Any,
    channel_id: str,
    channel_name: str,
    channel_url: str,
    run_report: dict[str, Any] | None = None,
) -> dict[str, str]:
    context = {
        'channel_name': channel_name,
        'channel_id': channel_id,
        'source_channel_url': channel_url,
        'channel_custom_url': '',
        'channel_country': '',
        'channel_published_at': '',
        'subscriber_count_snapshot': '0',
        'channel_total_views_snapshot': '0',
        'channel_video_count_snapshot': '0',
    }

    try:
        if run_report is not None:
            increment_api_call(run_report, 'channels_list')
        response = youtube.channels().list(
            part='snippet,statistics',
            id=channel_id,
            maxResults=1,
        ).execute()
        item = (response.get('items') or [{}])[0]
        snippet = item.get('snippet', {})
        statistics = item.get('statistics', {})

        context.update({
            'channel_custom_url': normalize_text(snippet.get('customUrl')),
            'channel_country': normalize_text(snippet.get('country')),
            'channel_published_at': normalize_text(snippet.get('publishedAt')),
            'subscriber_count_snapshot': normalize_text(statistics.get('subscriberCount', 0)),
            'channel_total_views_snapshot': normalize_text(statistics.get('viewCount', 0)),
            'channel_video_count_snapshot': normalize_text(statistics.get('videoCount', 0)),
        })
    except Exception as exc:
        logger.warning(
            'No se pudo obtener el contexto extendido del canal %s (%s): %s',
            channel_name,
            channel_id,
            str(exc),
        )
        logger.warning(traceback.format_exc())

    return context



def build_video_record(
    item: dict[str, Any],
    channel_context: dict[str, str],
    execution_time: datetime | None = None,
) -> dict[str, str]:
    execution_time = execution_time or utc_now()
    snippet = item.get('snippet', {})
    statistics = item.get('statistics', {})
    content_details = item.get('contentDetails', {})
    status = item.get('status', {})

    duration_iso = normalize_text(content_details.get('duration', 'PT0S')) or 'PT0S'
    duration_seconds = iso_duration_to_seconds(duration_iso)
    upload_datetime = parse_upload_datetime(snippet.get('publishedAt'))
    upload_date = upload_datetime.strftime('%Y-%m-%dT%H:%M:%S') if upload_datetime else ''
    published_at_utc = upload_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') if upload_datetime else ''
    days_since_upload = str(max((execution_time - upload_datetime).days, 0)) if upload_datetime else ''
    views = int(statistics.get('viewCount', 0) or 0)
    likes = int(statistics.get('likeCount', 0) or 0)
    comments = int(statistics.get('commentCount', 0) or 0)
    engagement_rate = f'{((likes + comments) / views):.6f}' if views > 0 else '0.000000'
    video_id = normalize_text(item.get('id'))

    record = {
        **channel_context,
        'video_id': video_id,
        'video_url': f'https://www.youtube.com/watch?v={video_id}' if video_id else '',
        'title': normalize_text(snippet.get('title')),
        'description': normalize_text(snippet.get('description')),
        'upload_date': upload_date,
        'published_at_utc': published_at_utc,
        'days_since_upload': days_since_upload,
        'tags': ','.join(snippet.get('tags', [])) if snippet.get('tags') else '',
        'thumbnail_url': normalize_text(snippet.get('thumbnails', {}).get('high', {}).get('url')),
        'duration_iso': duration_iso,
        'duration_seconds': str(duration_seconds),
        'dimension': normalize_text(content_details.get('dimension')),
        'definition': normalize_text(content_details.get('definition')),
        'caption': normalize_text(content_details.get('caption')),
        'licensed_content': normalize_text(content_details.get('licensedContent')),
        'projection': normalize_text(content_details.get('projection')),
        'privacy_status': normalize_text(status.get('privacyStatus')),
        'license': normalize_text(status.get('license')),
        'embeddable': normalize_text(status.get('embeddable')),
        'public_stats_viewable': normalize_text(status.get('publicStatsViewable')),
        'made_for_kids': normalize_text(status.get('madeForKids')),
        'self_declared_made_for_kids': normalize_text(status.get('selfDeclaredMadeForKids')),
        'category_id': normalize_text(snippet.get('categoryId')),
        'default_language': normalize_text(snippet.get('defaultLanguage')),
        'default_audio_language': normalize_text(snippet.get('defaultAudioLanguage')),
        'live_broadcast_content': normalize_text(snippet.get('liveBroadcastContent')),
        'views': str(views),
        'likes': str(likes),
        'comments': str(comments),
        'engagement_rate': engagement_rate,
        'execution_date': execution_time.strftime('%Y-%m-%d'),
        'fetched_at': execution_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
    }

    return {column: normalize_text(record.get(column, '')) for column in EXPORT_COLUMNS}



def prepare_dataframe_for_export(df: pd.DataFrame) -> pd.DataFrame:
    return prepare_dataframe_with_columns(df, EXPORT_COLUMNS)



def prepare_channel_registry_for_export(df: pd.DataFrame) -> pd.DataFrame:
    registry_df = prepare_dataframe_with_columns(df, CHANNEL_REGISTRY_COLUMNS)
    if not registry_df.empty:
        registry_df['active'] = registry_df['active'].map(normalize_bool_text)
    return registry_df



def log_dataframe_sample(df: pd.DataFrame, label: str = 'muestra') -> None:
    if df.empty:
        logger.info('No hay registros disponibles para mostrar una muestra de %s.', label)
        return

    sample = df.head(1).to_dict(orient='records')[0]
    logger.info('Muestra real procesada (%s): %s', label, json.dumps(sample, ensure_ascii=False))



def ensure_sheet_capacity(sheet: Any, required_rows: int, required_cols: int) -> None:
    current_rows = getattr(sheet, 'row_count', 0)
    current_cols = getattr(sheet, 'col_count', 0)

    if current_rows >= required_rows and current_cols >= required_cols:
        logger.info(
            'La hoja "%s" ya tiene espacio suficiente (%s filas x %s columnas). Requerido: %s x %s.',
            getattr(sheet, 'title', '<sin título>'),
            current_rows,
            current_cols,
            required_rows,
            required_cols,
        )
        return

    new_rows = max(current_rows, required_rows)
    new_cols = max(current_cols, required_cols)
    logger.warning(
        'La hoja "%s" no tiene espacio suficiente. Actual: %s x %s. Requerido: %s x %s. Redimensionando a %s x %s.',
        getattr(sheet, 'title', '<sin título>'),
        current_rows,
        current_cols,
        required_rows,
        required_cols,
        new_rows,
        new_cols,
    )
    sheet.resize(rows=new_rows, cols=new_cols)
    logger.info('Hoja "%s" redimensionada correctamente.', getattr(sheet, 'title', '<sin título>'))



def get_or_create_worksheet(spreadsheet: Any, title: str, rows: int = 1000, cols: int = 50) -> Any:
    try:
        worksheet = spreadsheet.worksheet(title)
        logger.info('Usando worksheet existente: "%s".', title)
        return worksheet
    except gspread.WorksheetNotFound:
        logger.info('La worksheet "%s" no existe. Creándola.', title)
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)



def write_replace_sheet(sheet: Any, df: pd.DataFrame) -> None:
    export_df = prepare_dataframe_for_export(df)
    required_rows = max(len(export_df) + 1, 1)
    required_cols = max(len(EXPORT_COLUMNS), 1)
    ensure_sheet_capacity(sheet, required_rows=required_rows, required_cols=required_cols)
    payload = [EXPORT_COLUMNS] + export_df.values.tolist()
    sheet.clear()
    sheet.update(payload)
    logger.info('Worksheet "%s" reemplazada con %s registros.', getattr(sheet, 'title', '<sin título>'), len(export_df))



def append_sheet_rows(sheet: Any, df: pd.DataFrame) -> None:
    export_df = prepare_dataframe_for_export(df)
    if export_df.empty:
        logger.info('No hay registros para append en la worksheet "%s".', getattr(sheet, 'title', '<sin título>'))
        return

    existing_header = []
    try:
        existing_header = sheet.row_values(1)
    except Exception:
        existing_header = []

    if existing_header != EXPORT_COLUMNS:
        write_replace_sheet(sheet, pd.DataFrame(columns=EXPORT_COLUMNS))

    required_rows = max(getattr(sheet, 'row_count', 0), len(export_df) + 1)
    ensure_sheet_capacity(sheet, required_rows=required_rows, required_cols=len(EXPORT_COLUMNS))
    sheet.append_rows(export_df.values.tolist(), value_input_option='RAW')
    logger.info('Se agregaron %s registros a la worksheet "%s".', len(export_df), getattr(sheet, 'title', '<sin título>'))



def load_channel_registry(spreadsheet: Any) -> pd.DataFrame:
    sheet = get_or_create_worksheet(spreadsheet, CHANNEL_REGISTRY_SHEET)

    try:
        records = sheet.get_all_records()
    except Exception as exc:
        logger.warning('No se pudo leer la worksheet "%s": %s', sheet.title, str(exc))
        records = []

    if not records:
        return pd.DataFrame(columns=CHANNEL_REGISTRY_COLUMNS)

    return prepare_channel_registry_for_export(pd.DataFrame(records))



def write_channel_registry(spreadsheet: Any, df: pd.DataFrame) -> None:
    sheet = get_or_create_worksheet(spreadsheet, CHANNEL_REGISTRY_SHEET)
    registry_df = prepare_channel_registry_for_export(df)
    required_rows = max(len(registry_df) + 1, 1)
    required_cols = len(CHANNEL_REGISTRY_COLUMNS)
    ensure_sheet_capacity(sheet, required_rows=required_rows, required_cols=required_cols)
    payload = [CHANNEL_REGISTRY_COLUMNS] + registry_df.values.tolist()
    sheet.clear()
    sheet.update(payload)
    logger.info('Channel registry actualizado con %s filas.', len(registry_df))



def build_channel_registry_error_row(source_url: str, normalized_url: str, resolver_type: str, error_message: str) -> dict[str, str]:
    return {
        'source_url': source_url,
        'normalized_url': normalized_url,
        'channel_id': '',
        'uploads_playlist_id': '',
        'channel_name': '',
        'resolver_type': resolver_type,
        'active': 'true',
        'resolution_status': 'error',
        'last_verified_at': utc_now_iso(),
        'last_error': normalize_text(error_message),
    }



def build_channel_registry_row(
    source_url: str,
    normalized_url: str,
    channel_item: dict[str, Any],
    resolver_type: str,
) -> dict[str, str]:
    snippet = channel_item.get('snippet', {})
    content_details = channel_item.get('contentDetails', {})
    related_playlists = content_details.get('relatedPlaylists', {})

    return {
        'source_url': source_url,
        'normalized_url': normalized_url,
        'channel_id': normalize_text(channel_item.get('id')),
        'uploads_playlist_id': normalize_text(related_playlists.get('uploads')),
        'channel_name': normalize_text(snippet.get('title')),
        'resolver_type': resolver_type,
        'active': 'true',
        'resolution_status': 'resolved',
        'last_verified_at': utc_now_iso(),
        'last_error': '',
    }



def fetch_channel_item(
    youtube: Any,
    run_report: dict[str, Any] | None,
    *,
    resolver_type: str,
    id: str | None = None,
    for_username: str | None = None,
    for_handle: str | None = None,
) -> tuple[dict[str, Any] | None, str]:
    if run_report is not None:
        increment_api_call(run_report, 'channels_list')
    response = youtube.channels().list(
        part='snippet,contentDetails',
        id=id,
        forUsername=for_username,
        forHandle=for_handle,
        maxResults=1,
    ).execute()
    items = response.get('items') or []
    if not items:
        return None, resolver_type
    return items[0], resolver_type



def resolve_channel_by_search(youtube: Any, normalized_url: str, run_report: dict[str, Any] | None = None) -> tuple[dict[str, Any] | None, str]:
    path = urlsplit(normalized_url).path.rstrip('/')
    query = path.split('/')[-1].lstrip('@')
    if not query:
        return None, 'search_fallback'

    if run_report is not None:
        increment_api_call(run_report, 'search_list')
    response = youtube.search().list(
        part='snippet',
        q=query,
        type='channel',
        maxResults=1,
    ).execute()
    items = response.get('items') or []
    if not items:
        return None, 'search_fallback'

    channel_id = normalize_text(items[0].get('snippet', {}).get('channelId'))
    if not channel_id:
        return None, 'search_fallback'

    return fetch_channel_item(youtube, run_report, resolver_type='search_fallback', id=channel_id)



def follow_channel_redirect(url: str) -> str:
    response = requests.get(url, timeout=HTTP_REDIRECT_TIMEOUT_SECONDS, allow_redirects=True)
    return normalize_channel_url(response.url)



def resolve_channel_url(
    youtube: Any,
    source_url: str,
    normalized_url: str | None = None,
    allow_redirect_fallback: bool = True,
    run_report: dict[str, Any] | None = None,
) -> dict[str, str]:
    normalized_url = normalized_url or normalize_channel_url(source_url)
    path = urlsplit(normalized_url).path.rstrip('/')

    try:
        if '/channel/' in path:
            channel_id = path.split('/channel/')[1].split('/')[0]
            channel_item, resolver_type = fetch_channel_item(youtube, run_report, resolver_type='channel_id', id=channel_id)
        elif '/user/' in path:
            username = path.split('/user/')[1].split('/')[0]
            channel_item, resolver_type = fetch_channel_item(youtube, run_report, resolver_type='username', for_username=username)
        elif path.startswith('/@') or '/@' in path:
            handle = path.split('@', 1)[1].split('/')[0]
            channel_item, resolver_type = fetch_channel_item(youtube, run_report, resolver_type='handle', for_handle=handle)
        else:
            channel_item = None
            resolver_type = 'unknown'

        if channel_item:
            return build_channel_registry_row(source_url, normalized_url, channel_item, resolver_type)

        if allow_redirect_fallback:
            redirected_url = follow_channel_redirect(normalized_url)
            if redirected_url and redirected_url != normalized_url:
                redirected_resolution = resolve_channel_url(
                    youtube,
                    source_url=source_url,
                    normalized_url=redirected_url,
                    allow_redirect_fallback=False,
                    run_report=run_report,
                )
                if redirected_resolution.get('resolution_status') == 'resolved':
                    redirected_resolution['normalized_url'] = redirected_url
                    redirected_resolution['resolver_type'] = f'redirect_{redirected_resolution["resolver_type"]}'
                    return redirected_resolution

        channel_item, resolver_type = resolve_channel_by_search(youtube, normalized_url, run_report=run_report)
        if channel_item:
            return build_channel_registry_row(source_url, normalized_url, channel_item, resolver_type)

        return build_channel_registry_error_row(
            source_url,
            normalized_url,
            resolver_type='unresolved',
            error_message=f'No se pudo resolver la URL del canal: {normalized_url}',
        )
    except Exception as exc:
        logger.warning('Error resolviendo canal %s: %s', normalized_url, str(exc))
        logger.warning(traceback.format_exc())
        return build_channel_registry_error_row(
            source_url,
            normalized_url,
            resolver_type='exception',
            error_message=str(exc),
        )



def deduplicate_channel_registry(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return prepare_channel_registry_for_export(df)

    registry_df = prepare_channel_registry_for_export(df)
    registry_df['active_rank'] = registry_df['active'].map(lambda value: 1 if normalize_bool_text(value) == 'true' else 0)
    registry_df['resolved_rank'] = registry_df['resolution_status'].map(lambda value: 1 if value == 'resolved' else 0)
    registry_df = registry_df.sort_values(
        by=['resolved_rank', 'active_rank', 'last_verified_at', 'normalized_url'],
        ascending=[False, False, False, True],
        kind='stable',
    )

    resolved_rows = registry_df[registry_df['channel_id'] != ''].drop_duplicates(subset=['channel_id'], keep='first')
    unresolved_rows = registry_df[registry_df['channel_id'] == ''].drop_duplicates(subset=['normalized_url'], keep='first')
    deduplicated = pd.concat([resolved_rows, unresolved_rows], ignore_index=True)
    deduplicated = deduplicated.drop(columns=['active_rank', 'resolved_rank'], errors='ignore')
    deduplicated = deduplicated.sort_values(by=['active', 'channel_name', 'normalized_url'], ascending=[False, True, True], kind='stable')
    return prepare_channel_registry_for_export(deduplicated)



def sync_channel_registry_from_urls(
    youtube: Any,
    spreadsheet: Any,
    urls: list[str],
    run_report: dict[str, Any] | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    registry_df = load_channel_registry(spreadsheet)
    registry_by_url = {record['normalized_url']: record for record in registry_df.to_dict(orient='records')} if not registry_df.empty else {}
    normalized_urls = {normalize_channel_url(url) for url in urls}
    desired_rows: list[dict[str, str]] = []

    for source_url in urls:
        normalized_url = normalize_channel_url(source_url)
        cached_row = registry_by_url.get(normalized_url)

        if (
            not force_refresh
            and cached_row
            and cached_row.get('channel_id')
            and cached_row.get('uploads_playlist_id')
            and cached_row.get('resolution_status') == 'resolved'
        ):
            cached_copy = dict(cached_row)
            cached_copy['source_url'] = source_url
            cached_copy['normalized_url'] = normalized_url
            cached_copy['active'] = 'true'
            desired_rows.append(cached_copy)
            continue

        resolved_row = resolve_channel_url(
            youtube,
            source_url=source_url,
            normalized_url=normalized_url,
            run_report=run_report,
        )
        if run_report is not None and resolved_row.get('resolution_status') == 'resolved':
            run_report['new_channels_resolved_this_run'] += 1
        desired_rows.append(resolved_row)

    inactive_rows: list[dict[str, str]] = []
    for normalized_url, row in registry_by_url.items():
        if normalized_url in normalized_urls:
            continue
        inactive_row = dict(row)
        inactive_row['active'] = 'false'
        inactive_rows.append(inactive_row)

    combined_df = pd.DataFrame(desired_rows + inactive_rows, columns=CHANNEL_REGISTRY_COLUMNS)
    combined_df = deduplicate_channel_registry(combined_df)
    write_channel_registry(spreadsheet, combined_df)
    return combined_df



def get_active_resolved_channels(registry_df: pd.DataFrame) -> pd.DataFrame:
    if registry_df.empty:
        return registry_df

    active_channels = registry_df[
        (registry_df['active'].map(normalize_bool_text) == 'true')
        & (registry_df['resolution_status'] == 'resolved')
        & (registry_df['channel_id'] != '')
        & (registry_df['uploads_playlist_id'] != '')
    ].copy()

    if active_channels.empty:
        return active_channels

    return active_channels.drop_duplicates(subset=['channel_id'], keep='first').reset_index(drop=True)



def filter_channels_dataframe(channels_df: pd.DataFrame, channel_filter: str = '', channel_limit: int | None = None) -> pd.DataFrame:
    filtered_df = channels_df.copy()
    normalized_filter = channel_filter.strip().lower()

    if normalized_filter and not filtered_df.empty:
        mask = (
            filtered_df['channel_name'].fillna('').str.lower().str.contains(normalized_filter, regex=False)
            | filtered_df['normalized_url'].fillna('').str.lower().str.contains(normalized_filter, regex=False)
            | filtered_df['channel_id'].fillna('').str.lower().str.contains(normalized_filter, regex=False)
        )
        filtered_df = filtered_df[mask].copy()

    if channel_limit is not None:
        filtered_df = filtered_df.head(channel_limit).copy()

    return filtered_df.reset_index(drop=True)



def list_recent_video_ids_from_uploads(
    youtube: Any,
    uploads_playlist_id: str,
    days: int = DEFAULT_LOOKBACK_DAYS,
    run_report: dict[str, Any] | None = None,
) -> list[str]:
    cutoff_datetime = utc_now() - timedelta(days=days)
    video_ids: list[str] = []
    seen_video_ids: set[str] = set()
    next_page_token = None

    while True:
        if run_report is not None:
            increment_api_call(run_report, 'playlistitems_list')
        response = youtube.playlistItems().list(
            part='snippet,contentDetails,status',
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token,
        ).execute()
        items = response.get('items') or []
        if not items:
            break

        for item in items:
            content_details = item.get('contentDetails', {})
            video_id = normalize_text(content_details.get('videoId'))
            published_at = content_details.get('videoPublishedAt') or item.get('snippet', {}).get('publishedAt')
            published_datetime = parse_upload_datetime(published_at)

            if not video_id:
                continue
            if published_datetime and published_datetime < cutoff_datetime:
                continue
            if video_id in seen_video_ids:
                continue

            seen_video_ids.add(video_id)
            video_ids.append(video_id)

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    if run_report is not None:
        run_report['videos_discovered'] += len(video_ids)
    return video_ids



def fetch_video_details_batch(youtube: Any, video_ids: list[str], run_report: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    if not video_ids:
        return []
    if run_report is not None:
        increment_api_call(run_report, 'videos_list')
    response = youtube.videos().list(
        part='snippet,contentDetails,statistics,status',
        id=','.join(video_ids),
    ).execute()
    return response.get('items') or []



def get_channel_videos(
    youtube: Any,
    channel_id: str,
    channel_name: str,
    channel_url: str = '',
    uploads_playlist_id: str = '',
    days: int = DEFAULT_LOOKBACK_DAYS,
    run_report: dict[str, Any] | None = None,
) -> pd.DataFrame:
    channel_context = fetch_channel_context(
        youtube,
        channel_id=channel_id,
        channel_name=channel_name,
        channel_url=channel_url,
        run_report=run_report,
    )
    execution_time = utc_now()
    result_df = pd.DataFrame(columns=EXPORT_COLUMNS)
    result_df.attrs['channel_fetch_failed'] = False

    try:
        recent_video_ids = list_recent_video_ids_from_uploads(
            youtube,
            uploads_playlist_id=uploads_playlist_id,
            days=days,
            run_report=run_report,
        )
        logger.info('Descubiertos %s videos recientes para %s desde uploads playlist.', len(recent_video_ids), channel_name)
    except Exception as exc:
        logger.error('Error al descubrir videos desde uploads playlist para %s: %s', channel_name, str(exc))
        logger.error(traceback.format_exc())
        result_df.attrs['channel_fetch_failed'] = True
        return result_df

    if not recent_video_ids:
        logger.info('No se encontraron videos recientes para el canal %s en los últimos %s días.', channel_name, days)
        return result_df

    videos: list[dict[str, str]] = []
    try:
        for index in range(0, len(recent_video_ids), 50):
            batch_video_ids = recent_video_ids[index:index + 50]
            video_items = fetch_video_details_batch(youtube, batch_video_ids, run_report=run_report)
            for item in video_items:
                videos.append(build_video_record(item, channel_context, execution_time=execution_time))
    except Exception as exc:
        logger.error('Error al enriquecer videos para %s: %s', channel_name, str(exc))
        logger.error(traceback.format_exc())
        result_df.attrs['channel_fetch_failed'] = True
        return result_df

    if not videos:
        logger.info('No se pudieron enriquecer videos para el canal %s.', channel_name)
        return result_df

    result_df = prepare_dataframe_for_export(pd.DataFrame(videos))
    result_df.attrs['channel_fetch_failed'] = False
    logger.info('DataFrame creado con %s registros para el canal %s.', len(result_df), channel_name)
    log_dataframe_sample(result_df, label=f'canal {channel_name}')
    return result_df



def append_videos_history(spreadsheet: Any, df: pd.DataFrame) -> int:
    history_df = prepare_dataframe_for_export(df)
    if history_df.empty:
        return 0

    history_sheet = get_or_create_worksheet(spreadsheet, VIDEOS_HISTORY_SHEET)
    try:
        existing_records = history_sheet.get_all_records()
    except Exception as exc:
        logger.warning('No se pudo leer videos_history antes del append: %s', str(exc))
        existing_records = []

    existing_keys = {
        f"{normalize_text(record.get('video_id'))}::{normalize_text(record.get('execution_date'))}"
        for record in existing_records
    }
    history_df = history_df.copy()
    history_df['_history_key'] = history_df['video_id'] + '::' + history_df['execution_date']
    history_append_df = history_df[~history_df['_history_key'].isin(existing_keys)].drop(columns=['_history_key'])

    if history_append_df.empty:
        logger.info('No hay filas nuevas para append en videos_history.')
        return 0

    append_sheet_rows(history_sheet, history_append_df)
    return len(history_append_df)



def export_dataframe_to_sheet(sheet: Any, combined_df: pd.DataFrame) -> None:
    write_replace_sheet(sheet, combined_df)



def record_channel_failure(run_report: dict[str, Any], channel: dict[str, str], error_message: str) -> None:
    run_report['channels_failed'] += 1
    run_report.setdefault('channel_failures', []).append({
        'channel_id': channel.get('channel_id', ''),
        'channel_name': channel.get('channel_name', ''),
        'normalized_url': channel.get('normalized_url', ''),
        'error': normalize_text(error_message),
    })



def main() -> int:
    started_at = utc_now_iso()
    run_report = create_run_report(started_at)

    def finalize(exit_code: int) -> int:
        run_report['finished_at'] = utc_now_iso()
        write_operational_artifacts(run_report)
        return exit_code

    try:
        days = parse_positive_int_env('DAYS', DEFAULT_LOOKBACK_DAYS)
        channel_limit = parse_optional_positive_int_env('CHANNEL_LIMIT')
        channel_filter = os.environ.get('CHANNEL_FILTER', '').strip()
        force_refresh_registry = parse_bool_env('FORCE_REFRESH_REGISTRY', default=False)
        write_history = parse_bool_env('WRITE_HISTORY', default=False)
    except ValueError as exc:
        logger.error(str(exc))
        return finalize(1)

    run_report['days_window'] = days
    run_report['channel_limit_applied'] = '' if channel_limit is None else str(channel_limit)
    run_report['channel_filter_applied'] = channel_filter
    run_report['write_history_enabled'] = 'true' if write_history else 'false'

    api_key = os.environ.get('YOUTUBE_API_KEY')
    if not api_key:
        logger.error("La clave de API no está configurada en la variable de entorno 'YOUTUBE_API_KEY'")
        return finalize(1)

    google_creds_json = os.environ.get('GOOGLE_SHEETS_CREDS_BASE64')
    if not google_creds_json:
        logger.error("Las credenciales de Google Sheets no están configuradas en 'GOOGLE_SHEETS_CREDS_BASE64'")
        return finalize(1)

    try:
        decoded_creds = base64.b64decode(google_creds_json)
        creds_dict = json.loads(decoded_creds)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive',
            ],
        )
        gc = gspread.authorize(credentials)
    except Exception as exc:
        logger.error(f'Error al cargar las credenciales de Google Sheets: {str(exc)}')
        logger.error(traceback.format_exc())
        return finalize(1)

    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    if not spreadsheet_id:
        logger.error("El ID de la hoja de cálculo no está configurado en 'SPREADSHEET_ID'")
        return finalize(1)

    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        current_sheet = get_or_create_worksheet(spreadsheet, VIDEOS_CURRENT_SHEET)
        get_or_create_worksheet(spreadsheet, CHANNEL_REGISTRY_SHEET)
        if write_history:
            get_or_create_worksheet(spreadsheet, VIDEOS_HISTORY_SHEET)
        logger.info(
            'Destino Google Sheets identificado. Documento: "%s" | Hoja snapshot actual: "%s".',
            spreadsheet.title,
            current_sheet.title,
        )
    except Exception as exc:
        logger.error(f'Error al abrir la hoja de cálculo: {str(exc)}')
        logger.error(traceback.format_exc())
        return finalize(1)

    youtube = build('youtube', 'v3', developerKey=api_key)

    channel_urls = get_urls()
    if not channel_urls:
        logger.info('No hay canales configurados en CHANNEL_URLS. Terminando ejecución.')
        return finalize(0)

    registry_df = sync_channel_registry_from_urls(
        youtube,
        spreadsheet,
        channel_urls,
        run_report=run_report,
        force_refresh=force_refresh_registry,
    )
    if not registry_df.empty:
        run_report['registry_rows_total'] = len(registry_df)
        run_report['registry_rows_active'] = int((registry_df['active'].map(normalize_bool_text) == 'true').sum())
        run_report['registry_rows_failed'] = int((registry_df['resolution_status'] != 'resolved').sum())
        unresolved_registry = registry_df[registry_df['resolution_status'] != 'resolved']
        run_report['unresolved_urls'] = unresolved_registry['normalized_url'].tolist() if not unresolved_registry.empty else []

    active_channels_df = get_active_resolved_channels(registry_df)
    active_channels_df = filter_channels_dataframe(active_channels_df, channel_filter=channel_filter, channel_limit=channel_limit)
    run_report['channels_total'] = len(active_channels_df)

    all_videos_df = pd.DataFrame(columns=EXPORT_COLUMNS)

    for channel in active_channels_df.to_dict(orient='records'):
        channel_df = get_channel_videos(
            youtube,
            channel_id=channel['channel_id'],
            channel_name=channel['channel_name'],
            channel_url=channel['normalized_url'],
            uploads_playlist_id=channel['uploads_playlist_id'],
            days=days,
            run_report=run_report,
        )
        if channel_df.attrs.get('channel_fetch_failed'):
            record_channel_failure(run_report, channel, 'Falló la extracción o enriquecimiento de videos.')
            continue

        run_report['channels_processed'] += 1
        if not channel_df.empty:
            all_videos_df = pd.concat([all_videos_df, channel_df], ignore_index=True)
            logger.info('Datos agregados para el canal: %s', channel['channel_name'])
        else:
            logger.info('Canal %s procesado sin videos en la ventana actual.', channel['channel_name'])

    current_snapshot_df = prepare_dataframe_for_export(all_videos_df)
    run_report['rows_current_snapshot'] = len(current_snapshot_df)
    run_report['videos_exported'] = len(current_snapshot_df)

    try:
        logger.info(
            'Canales procesados. %s canales totales con %s registros en el snapshot actual.',
            len(current_snapshot_df['channel_name'].replace('', pd.NA).dropna().unique()) if not current_snapshot_df.empty else 0,
            len(current_snapshot_df),
        )
        logger.info('Snapshot actual preparado. %s registros listos para exportar.', len(current_snapshot_df))
        log_dataframe_sample(current_snapshot_df, label='exportación final')
    except Exception as exc:
        logger.error(f'Error al preparar el DataFrame final: {str(exc)}')
        logger.error(traceback.format_exc())
        return finalize(1)

    try:
        write_replace_sheet(current_sheet, current_snapshot_df)
        if write_history:
            run_report['history_rows_appended'] = append_videos_history(spreadsheet, current_snapshot_df)
    except Exception as exc:
        logger.error(f'Error al actualizar la hoja de cálculo: {str(exc)}')
        logger.error(traceback.format_exc())
        return finalize(1)

    return finalize(0)


if __name__ == '__main__':
    raise SystemExit(main())
