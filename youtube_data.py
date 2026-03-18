import base64
import json
import logging
import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any

import gspread
import isodate
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from get_urls import get_urls

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



def iso_duration_to_seconds(duration: str) -> int:
    try:
        parsed_duration = isodate.parse_duration(duration)
        return int(parsed_duration.total_seconds())
    except Exception as exc:
        logger.error(f"Error al convertir la duración {duration}: {str(exc)}")
        logger.error(traceback.format_exc())
        return 0



def parse_upload_datetime(upload_date_str: str | None) -> datetime | None:
    if not upload_date_str:
        return None
    parsed_datetime = isodate.parse_datetime(upload_date_str)
    if parsed_datetime.tzinfo is None:
        parsed_datetime = parsed_datetime.replace(tzinfo=timezone.utc)
    return parsed_datetime.astimezone(timezone.utc)



def fetch_channel_context(youtube: Any, channel_id: str, channel_name: str, channel_url: str) -> dict[str, str]:
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
            "No se pudo obtener el contexto extendido del canal %s (%s): %s",
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
    days_since_upload = (
        str(max((execution_time - upload_datetime).days, 0))
        if upload_datetime
        else ''
    )
    views = int(statistics.get('viewCount', 0) or 0)
    likes = int(statistics.get('likeCount', 0) or 0)
    comments = int(statistics.get('commentCount', 0) or 0)
    engagement_rate = (
        f"{((likes + comments) / views):.6f}" if views > 0 else '0.000000'
    )
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
    export_df = df.copy()

    for column in EXPORT_COLUMNS:
        if column not in export_df.columns:
            export_df[column] = ''

    export_df = export_df[EXPORT_COLUMNS]
    export_df = export_df.fillna('')

    for column in export_df.columns:
        export_df[column] = export_df[column].map(normalize_text)

    return export_df



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



def get_channels() -> list[str]:
    return [
        "https://www.youtube.com/@MarianoTrejo",
        "https://www.youtube.com/@humphrey",
        "https://www.youtube.com/@MisPropiasFinanzas",
        "https://www.youtube.com/@AdriàSolàPastor",
        "https://www.youtube.com/@EduardoRosas",
        "https://www.youtube.com/@CésarDabiánFinanzas",
        "https://www.youtube.com/@soycristinadayz",
        "https://www.youtube.com/@MorisDieck",
        "https://www.youtube.com/@AdrianSaenz",
        "https://www.youtube.com/@FinanzasparatodosYT",
        "https://www.youtube.com/@LuisMiNegocios",
        "https://www.youtube.com/@AprendizFinanciero",
        "https://www.youtube.com/@negociosyfinanzas2559",
        "https://www.youtube.com/@pequenocerdocapitalista",
        "https://www.youtube.com/@AlexHormozi",
        "https://www.youtube.com/@CalebHammer",
        "https://www.youtube.com/c/Myprimermillón",
        "https://www.youtube.com/@starterstory",
        "https://www.youtube.com/@irenealbacete",
        "https://www.youtube.com/@bulkin_uri",
        "https://www.youtube.com/@ExitoFinancieroOficial",
        "https://www.youtube.com/@compuestospodcast",
        "https://www.youtube.com/@JaimeHigueraEspanol",
        "https://www.youtube.com/@soycristinadayz",
        "https://www.youtube.com/@jefillysh",
        "https://www.youtube.com/@EDteam",
        "https://www.youtube.com/@LatinoSueco",
        "https://www.youtube.com/@Ter",
        "https://www.youtube.com/@doctorfision",
        "https://www.youtube.com/@EvaMariaBeristain",
        "https://www.youtube.com/@Unicoos",
        "https://www.youtube.com/@QuantumFracture",
        "https://www.youtube.com/@lagatadeschrodinger",
        "https://www.youtube.com/@CdeCiencia",
        "https://www.youtube.com/@elrobotdeplaton",
        "https://www.youtube.com/@PhysicsGirl",
        "https://www.youtube.com/@Veritasium",
        "https://www.youtube.com/@Vsauce",
        "https://www.youtube.com/@minutephysics",
        "https://www.youtube.com/@SmarterEveryDay",
        "https://www.youtube.com/@AsapSCIENCE",
        "https://www.youtube.com/@PBSSpaceTime",
        "https://www.youtube.com/@TheActionLab",
        "https://www.youtube.com/@numberphile",
        "https://www.youtube.com/@crashcourse",
        "https://www.youtube.com/@AliAbdaal",
        "https://www.youtube.com/@ThomasFrank",
        "https://www.youtube.com/@MattDAvella",
        "https://www.youtube.com/@NathanielDrew",
        "https://www.youtube.com/@LordDraugr",
        "https://www.youtube.com/@danielfelipemedina",
        "https://www.youtube.com/@omareducacionfinanciera",
        "https://www.youtube.com/@TwoMinutePapers",
        "https://www.youtube.com/@ElConsejeronocturno",
    ]



def get_channel_videos(api_key: str, channel_id: str, channel_name: str, channel_url: str = '', days: int = 60) -> pd.DataFrame:
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
    except Exception as exc:
        logger.error(f"Error al inicializar el cliente de YouTube API: {str(exc)}")
        return pd.DataFrame(columns=EXPORT_COLUMNS)

    try:
        cutoff_datetime = utc_now() - timedelta(days=days)
        cutoff_date = cutoff_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
        logger.info('Fecha de corte calculada para %s: %s', channel_name, cutoff_date)
    except Exception as exc:
        logger.error(f"Error al calcular la fecha de corte: {str(exc)}")
        return pd.DataFrame(columns=EXPORT_COLUMNS)

    channel_context = fetch_channel_context(youtube, channel_id, channel_name, channel_url)
    execution_time = utc_now()
    videos: list[dict[str, str]] = []
    next_page_token = None

    while True:
        try:
            res = youtube.search().list(
                part='snippet',
                channelId=channel_id,
                publishedAfter=cutoff_date,
                maxResults=50,
                pageToken=next_page_token,
                order='date',
                type='video',
            ).execute()
            logger.info('Obtenidos videos de la página con token %s para %s.', next_page_token, channel_name)
        except Exception as exc:
            logger.error(f"Error al obtener videos del canal {channel_name}: {str(exc)}")
            break

        if not res.get('items'):
            logger.warning(f"La respuesta de búsqueda está vacía para el canal {channel_name}.")
            break

        try:
            video_ids = [item['id']['videoId'] for item in res.get('items', [])]
            if not video_ids:
                logger.info(f"No se encontraron más videos para el canal {channel_name}")
                break
        except KeyError as exc:
            logger.error(f"Error al extraer IDs de videos: {str(exc)}")
            break

        try:
            stats_res = youtube.videos().list(
                part='snippet,contentDetails,statistics,status',
                id=','.join(video_ids),
            ).execute()
            logger.info('Obtenidos detalles de %s videos para %s.', len(video_ids), channel_name)
        except Exception as exc:
            logger.error(f"Error al obtener detalles de videos: {str(exc)}")
            break

        if not stats_res.get('items'):
            logger.warning(f"No se obtuvieron detalles de videos para los IDs: {video_ids}")
            continue

        for item in stats_res.get('items', []):
            try:
                record = build_video_record(item, channel_context, execution_time=execution_time)
                videos.append(record)
                logger.info('Procesado video ID: %s', item.get('id'))
            except Exception as exc:
                logger.error(f"Error al procesar el video {item.get('id')}: {str(exc)}")
                logger.error(traceback.format_exc())
                continue

        next_page_token = res.get('nextPageToken')
        if not next_page_token:
            logger.info('Se han procesado todos los videos para el canal %s.', channel_name)
            break

        time.sleep(0.1)

    if not videos:
        logger.warning('No se encontraron videos para el canal %s en los últimos %s días.', channel_name, days)
        return pd.DataFrame(columns=EXPORT_COLUMNS)

    df = pd.DataFrame(videos)
    df = prepare_dataframe_for_export(df)
    logger.info('DataFrame creado con %s registros para el canal %s.', len(df), channel_name)
    log_dataframe_sample(df, label=f'canal {channel_name}')
    return df



def get_channel_id_and_name_from_url(youtube: Any, channel_url: str) -> tuple[str | None, str | None]:
    channel_id = None
    channel_name = None

    try:
        if '/channel/' in channel_url:
            channel_id = channel_url.split('/channel/')[1].split('/')[0]
            res = youtube.channels().list(
                part='id,snippet',
                id=channel_id,
            ).execute()
            if res.get('items'):
                channel_name = res['items'][0]['snippet']['title']
            else:
                logger.warning(f"No se encontró información para channel_id: {channel_id}")
        elif '/user/' in channel_url:
            username = channel_url.split('/user/')[1].split('/')[0]
            res = youtube.channels().list(
                part='id,snippet',
                forUsername=username,
            ).execute()
            if res.get('items'):
                channel_id = res['items'][0]['id']
                channel_name = res['items'][0]['snippet']['title']
            else:
                logger.warning(f"No se encontró información para el usuario: {username}")
        elif '@' in channel_url:
            handle = channel_url.split('@')[1].split('/')[0]
            res = youtube.channels().list(
                part='id,snippet',
                forUsername=handle,
            ).execute()
            if res.get('items'):
                channel_id = res['items'][0]['id']
                channel_name = res['items'][0]['snippet']['title']
            else:
                res = youtube.search().list(
                    part='snippet',
                    q=handle,
                    type='channel',
                    maxResults=1,
                ).execute()
                if res.get('items'):
                    channel_id = res['items'][0]['snippet']['channelId']
                    channel_name = res['items'][0]['snippet']['channelTitle']
                else:
                    logger.warning(f"No se encontró información para el handle: {handle}")
        else:
            logger.error(f"URL del canal no reconocida: {channel_url}")
            return None, None

        if channel_id and not channel_name:
            res = youtube.channels().list(
                part='snippet',
                id=channel_id,
            ).execute()
            if res.get('items'):
                channel_name = res['items'][0]['snippet']['title']
            else:
                logger.warning(f"No se encontró información para channel_id: {channel_id}")
    except Exception as exc:
        logger.error(f"Error al obtener el ID y nombre del canal desde {channel_url}: {str(exc)}")
        logger.error(traceback.format_exc())

    if not channel_id or not channel_name:
        logger.error(f"No se pudo obtener el ID o nombre del canal desde {channel_url}")
        return None, None

    return channel_id, channel_name



def export_dataframe_to_sheet(sheet: Any, combined_df: pd.DataFrame) -> None:
    export_df = prepare_dataframe_for_export(combined_df)
    required_rows = len(export_df) + 1
    required_cols = len(export_df.columns)
    ensure_sheet_capacity(sheet, required_rows=required_rows, required_cols=required_cols)
    sheet.clear()
    sheet.update([export_df.columns.values.tolist()] + export_df.values.tolist())
    logger.info('Datos actualizados en la hoja de cálculo "%s".', getattr(sheet, 'title', '<sin título>'))



def main() -> int:
    api_key = os.environ.get('YOUTUBE_API_KEY')
    if not api_key:
        logger.error("La clave de API no está configurada en la variable de entorno 'YOUTUBE_API_KEY'")
        return 1

    google_creds_json = os.environ.get('GOOGLE_SHEETS_CREDS_BASE64')
    if not google_creds_json:
        logger.error("Las credenciales de Google Sheets no están configuradas en 'GOOGLE_SHEETS_CREDS_BASE64'")
        return 1

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
        logger.error(f"Error al cargar las credenciales de Google Sheets: {str(exc)}")
        logger.error(traceback.format_exc())
        return 1

    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    if not spreadsheet_id:
        logger.error("El ID de la hoja de cálculo no está configurado en 'SPREADSHEET_ID'")
        return 1

    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        sheet = spreadsheet.sheet1
        logger.info(
            'Destino Google Sheets identificado. Documento: "%s" | Hoja: "%s".',
            spreadsheet.title,
            sheet.title,
        )
    except Exception as exc:
        logger.error(f"Error al abrir la hoja de cálculo: {str(exc)}")
        logger.error(traceback.format_exc())
        return 1

    youtube = build('youtube', 'v3', developerKey=api_key)

    try:
        existing_data = pd.DataFrame(sheet.get_all_records())
        existing_data = prepare_dataframe_for_export(existing_data) if not existing_data.empty else pd.DataFrame(columns=EXPORT_COLUMNS)
        logger.info('Datos existentes cargados desde "%s", %s registros encontrados.', sheet.title, len(existing_data))
    except Exception as exc:
        logger.warning(f"No se pudo leer datos existentes o la hoja está vacía: {str(exc)}")
        existing_data = pd.DataFrame(columns=EXPORT_COLUMNS)

    channel_urls = get_urls()
    if not channel_urls:
        logger.info('No hay canales para procesar el bloque semanal actual. Terminando ejecución.')
        return 0

    all_videos_df = pd.DataFrame(columns=EXPORT_COLUMNS)

    for url in channel_urls:
        channel_id, channel_name = get_channel_id_and_name_from_url(youtube, url)
        if channel_id and channel_name:
            df = get_channel_videos(api_key, channel_id, channel_name, channel_url=url, days=60)
            if not df.empty:
                all_videos_df = pd.concat([all_videos_df, df], ignore_index=True)
                logger.info('Datos agregados para el canal: %s', channel_name)
            else:
                logger.warning('No se encontraron videos para el canal: %s', channel_name)
        else:
            logger.error('No se pudo obtener el ID o nombre del canal para %s', url)

    if all_videos_df.empty:
        logger.error('No se encontraron videos para ninguno de los canales proporcionados en el bloque semanal actual.')
        return 1

    if not existing_data.empty:
        combined_df = pd.concat([existing_data, all_videos_df], ignore_index=True)
        combined_df.drop_duplicates(subset=['video_id', 'channel_id'], inplace=True)
    else:
        combined_df = all_videos_df

    combined_df = prepare_dataframe_for_export(combined_df)

    try:
        logger.info(
            'Canales procesados. %s canales totales con %s registros.',
            len(combined_df['channel_name'].replace('', pd.NA).dropna().unique()),
            len(combined_df),
        )
        logger.info('Filtrado de datos completado. %s registros listos para exportar.', len(combined_df))
        log_dataframe_sample(combined_df, label='exportación final')
    except Exception as exc:
        logger.error(f"Error al preparar el DataFrame final: {str(exc)}")
        logger.error(traceback.format_exc())
        return 1

    try:
        export_dataframe_to_sheet(sheet, combined_df)
    except Exception as exc:
        logger.error(f"Error al actualizar la hoja de cálculo: {str(exc)}")
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
