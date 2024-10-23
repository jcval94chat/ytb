# youtube_data.py

import os
import json
import pandas as pd
from datetime import datetime, timedelta, UTC  # Añadido UTC
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import gspread
import logging
import time
import isodate
import base64
import traceback

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Crear formato de logging
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Crear manejador para archivo
file_handler = logging.FileHandler('youtube_data.log', mode='a')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Crear manejador para consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)



def get_channels():

    channel_urls = [
        "https://www.youtube.com/@MarianoTrejo",
        "https://www.youtube.com/@humphrey",
        "https://www.youtube.com/@MisPropiasFinanzas",
        # "https://www.youtube.com/c/Adri%C3%A0Sol%C3%A0Pastor",
        "https://www.youtube.com/@AdriàSolàPastor",
        "https://www.youtube.com/@EduardoRosas",
        # "https://www.youtube.com/c/C%C3%A9sarDabiánFinanzas",
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
    ]
    return channel_urls


def get_channel_videos(api_key, channel_id, channel_name, days=90):
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
    except Exception as e:
        logging.error(f"Error al inicializar el cliente de YouTube API: {str(e)}")
        return pd.DataFrame()

    # Calcular la fecha de corte (formato RFC 3339 sin microsegundos)
    try:
        cutoff_datetime = datetime.now(UTC) - timedelta(days=days)  # Usando datetime.now(UTC)
        
        cutoff_date = cutoff_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
        logging.info(f"Fecha de corte calculada: {cutoff_date}")
    except Exception as e:
        logging.error(f"Error al calcular la fecha de corte: {str(e)}")
        return pd.DataFrame()

    videos = []
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
                type='video'
            ).execute()
            logging.info(f"Obtenidos videos de la página con token: {next_page_token}")
        except Exception as e:
            logging.error(f"Error al obtener videos del canal {channel_name}: {str(e)}")
            break

        # Verificar si res contiene items
        if not res.get('items'):
            logging.warning(f"La respuesta de búsqueda está vacía para el canal {channel_name}.")
            break

        try:
            video_ids = [item['id']['videoId'] for item in res.get('items', [])]
            if not video_ids:
                logging.info(f"No se encontraron más videos para el canal {channel_name}")
                break
        except KeyError as e:
            logging.error(f"Error al extraer IDs de videos: {str(e)}")
            break

        try:
            # Obtener detalles y estadísticas de los videos
            stats_res = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=','.join(video_ids)
            ).execute()
            logging.info(f"Obtenidos detalles de {len(video_ids)} videos")
        except Exception as e:
            logging.error(f"Error al obtener detalles de videos: {str(e)}")
            break

        # Verificar si stats_res contiene items
        if not stats_res.get('items'):
            logging.warning(f"No se obtuvieron detalles de videos para los IDs: {video_ids}")
            continue  # Pasar a la siguiente página

        for item in stats_res.get('items', []):
            try:
                snippet = item.get('snippet', {})
                statistics = item.get('statistics', {})
                content_details = item.get('contentDetails', {})

                # Convertir la duración de ISO 8601 a segundos
                duration_iso = content_details.get('duration', 'PT0S')
                duration_seconds = iso_duration_to_seconds(duration_iso)

                # Estandarizar la fecha de subida
                upload_date_str = snippet.get('publishedAt')
                if upload_date_str:
                    upload_date = isodate.parse_datetime(upload_date_str)
                    upload_date = upload_date.strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    upload_date = None
                    logging.warning(f"No se encontró 'publishedAt' para el video ID: {item.get('id')}")

                videos.append({
                    'channel_name': channel_name,
                    'video_id': item.get('id'),
                    'title': snippet.get('title'),
                    'description': snippet.get('description'),
                    'upload_date': upload_date,
                    'tags': ','.join(snippet.get('tags', [])),
                    'thumbnail_url': snippet.get('thumbnails', {}).get('high', {}).get('url'),
                    'duration_seconds': duration_seconds,
                    'views': int(statistics.get('viewCount', 0)),
                    'likes': int(statistics.get('likeCount', 0)),
                    'comments': int(statistics.get('commentCount', 0)),
                    'execution_date': datetime.utcnow().strftime('%Y-%m-%d')
                })
                logging.info(f"Procesado video ID: {item.get('id')}")
            except Exception as e:
                logging.error(f"Error al procesar el video {item.get('id')}: {str(e)}")
                logging.error(traceback.format_exc())
                continue  # Continuar con el siguiente video

        next_page_token = res.get('nextPageToken')
        if not next_page_token:
            logging.info(f"Se han procesado todos los videos para el canal {channel_name}")
            break

        # Respetar el límite de solicitudes por segundo
        time.sleep(0.1)

    if not videos:
        logging.warning(f"No se encontraron videos para el canal {channel_name} en los últimos {days} días.")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(videos)
        logging.info(f"DataFrame creado con {len(df)} registros para el canal {channel_name}")
    except Exception as e:
        logging.error(f"Error al crear el DataFrame: {str(e)}")
        df = pd.DataFrame()

    return df

def iso_duration_to_seconds(duration):
    try:
        parsed_duration = isodate.parse_duration(duration)
        return int(parsed_duration.total_seconds())
    except Exception as e:
        logging.error(f"Error al convertir la duración {duration}: {str(e)}")
        logging.error(traceback.format_exc())
        return 0

def get_channel_id_and_name_from_url(youtube, channel_url):
    channel_id = None
    channel_name = None

    try:
        if '/channel/' in channel_url:
            channel_id = channel_url.split('/channel/')[1].split('/')[0]
            res = youtube.channels().list(
                part='id,snippet',
                id=channel_id
            ).execute()
            if res.get('items'):
                channel_name = res['items'][0]['snippet']['title']
            else:
                logging.warning(f"No se encontró información para channel_id: {channel_id}")
        elif '/user/' in channel_url:
            username = channel_url.split('/user/')[1].split('/')[0]
            res = youtube.channels().list(
                part='id,snippet',
                forUsername=username
            ).execute()
            if res.get('items'):
                channel_id = res['items'][0]['id']
                channel_name = res['items'][0]['snippet']['title']
            else:
                logging.warning(f"No se encontró información para el usuario: {username}")
        elif '@' in channel_url:
            handle = channel_url.split('@')[1].split('/')[0]
            res = youtube.channels().list(
                part='id,snippet',
                forUsername=handle
            ).execute()
            if res.get('items'):
                channel_id = res['items'][0]['id']
                channel_name = res['items'][0]['snippet']['title']
            else:
                res = youtube.search().list(
                    part='snippet',
                    q=handle,
                    type='channel',
                    maxResults=1
                ).execute()
                if res.get('items'):
                    channel_id = res['items'][0]['snippet']['channelId']
                    channel_name = res['items'][0]['snippet']['channelTitle']
                else:
                    logging.warning(f"No se encontró información para el handle: {handle}")
        else:
            logging.error(f"URL del canal no reconocida: {channel_url}")
            return None, None

        if channel_id and not channel_name:
            res = youtube.channels().list(
                part='snippet',
                id=channel_id
            ).execute()
            if res.get('items'):
                channel_name = res['items'][0]['snippet']['title']
            else:
                logging.warning(f"No se encontró información para channel_id: {channel_id}")
    except Exception as e:
        logging.error(f"Error al obtener el ID y nombre del canal desde {channel_url}: {str(e)}")
        logging.error(traceback.format_exc())

    if not channel_id or not channel_name:
        logging.error(f"No se pudo obtener el ID o nombre del canal desde {channel_url}")
        return None, None

    return channel_id, channel_name

if __name__ == '__main__':
    api_key = os.environ.get('YOUTUBE_API_KEY')
    if not api_key:
        logging.error("La clave de API no está configurada en la variable de entorno 'YOUTUBE_API_KEY'")
        exit(1)

    # Cargar las credenciales de Google Sheets desde la variable de entorno
    google_creds_json = os.environ.get('GOOGLE_SHEETS_CREDS_BASE64')
    if not google_creds_json:
        logging.error("Las credenciales de Google Sheets no están configuradas en 'GOOGLE_SHEETS_CREDS_BASE64'")
        exit(1)

    # Decodificar las credenciales de base64
    try:
        decoded_creds = base64.b64decode(google_creds_json)
        creds_dict = json.loads(decoded_creds)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        gc = gspread.authorize(credentials)
    except Exception as e:
        logging.error(f"Error al cargar las credenciales de Google Sheets: {str(e)}")
        logging.error(traceback.format_exc())
        exit(1)

    # ID de la hoja de cálculo de Google Sheets
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    if not spreadsheet_id:
        logging.error("El ID de la hoja de cálculo no está configurado en 'SPREADSHEET_ID'")
        exit(1)

    try:
        sheet = gc.open_by_key(spreadsheet_id).sheet1  # Usamos la primera hoja
    except Exception as e:
        logging.error(f"Error al abrir la hoja de cálculo: {str(e)}")
        logging.error(traceback.format_exc())
        exit(1)

    youtube = build('youtube', 'v3', developerKey=api_key)

    # Leer los datos existentes en la hoja
    try:
        existing_data = pd.DataFrame(sheet.get_all_records())
        logging.info(f"Datos existentes cargados, {len(existing_data)} registros encontrados.")
    except Exception as e:
        logging.warning(f"No se pudo leer datos existentes o la hoja está vacía: {str(e)}")
        existing_data = pd.DataFrame()

    channel_urls = get_channels()

    all_videos_df = pd.DataFrame()

    for url in channel_urls:
        channel_id, channel_name = get_channel_id_and_name_from_url(youtube, url)
        if channel_id and channel_name:
            df = get_channel_videos(api_key, channel_id, channel_name, days=90)
            if not df.empty:
                all_videos_df = pd.concat([all_videos_df, df], ignore_index=True)
                logging.info(f"Datos agregados para el canal: {channel_name}")
            else:
                logging.warning(f"No se encontraron videos para el canal: {channel_name}")
        else:
            logging.error(f"No se pudo obtener el ID o nombre del canal para {url}")

    if all_videos_df.empty:
        logging.error("No se encontraron videos para ninguno de los canales proporcionados.")
        exit(1)

    # Combinar los datos nuevos con los existentes y eliminar duplicados
    if not existing_data.empty:
        combined_df = pd.concat([existing_data, all_videos_df], ignore_index=True)
        combined_df.drop_duplicates(subset=['video_id','channel_name'], inplace=True)
    else:
        combined_df = all_videos_df

    # Filtrar datos de los últimos 90 días
    try:
        logging.info(f"Canales procesados. {len(combined_df['channel_name'].unique())} canales totales con {len(combined_df)} registros")
        # combined_df['upload_date'] = pd.to_datetime(combined_df['upload_date'], format='%Y-%m-%dT%H:%M:%S')
        # cutoff_date = datetime.utcnow() - timedelta(days=90)
        # combined_df = combined_df[combined_df['upload_date'] >= cutoff_date]

        # Convertir las fechas
        # combined_df['upload_date'] = pd.to_datetime(combined_df['upload_date'], errors='coerce')  # No especificar el formato
        # combined_df = combined_df.dropna(subset=['upload_date'])
        # cutoff_date = datetime.utcnow() - timedelta(days=90)
        # combined_df = combined_df[combined_df['upload_date'] >= cutoff_date]
        
        logging.info(f"Filtrado de datos completado. {len(combined_df)} registros después del filtro de fecha.")
    except Exception as e:
        logging.error(f"Error al procesar las fechas de 'upload_date': {str(e)}")
        logging.error(traceback.format_exc())


    # Antes de actualizar la hoja, convierte las columnas datetime a strings
    datetime_columns = combined_df.select_dtypes(include=['datetime64[ns]']).columns
    for col in datetime_columns:
        combined_df[col] = combined_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    
    # Actualizar la hoja de cálculo con los datos combinados
    try:
        sheet.clear()
        sheet.update([combined_df.columns.values.tolist()] + combined_df.values.tolist())
        logging.info("Datos actualizados en la hoja de cálculo.")
    except Exception as e:
        logging.error(f"Error al actualizar la hoja de cálculo: {str(e)}")
        logging.error(traceback.format_exc())
