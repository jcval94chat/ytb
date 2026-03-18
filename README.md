# Extracción de datos de YouTube

Este repositorio extrae datos de vídeos recientes de una lista de canales de YouTube y publica un snapshot actual en Google Sheets.

## Interfaz manual única

La única lista que debes editar manualmente es `CHANNEL_URLS` en `get_urls.py`.

- Añade o elimina canales directamente en `CHANNEL_URLS`.
- Puedes usar URLs con `@handle`, `@handle/videos`, `/channel/UC...`, `/user/...`, `/c/...`, `m.youtube.com`, query params o fragmentos.
- No hay pasos manuales adicionales para registrar canales.

## Caché automática de resolución de canales

La worksheet `channel_registry` es una caché técnica interna mantenida automáticamente por el código.

- `CHANNEL_URLS` sigue siendo la fuente de verdad.
- Cada ejecución normaliza las URLs, resuelve cada canal a su forma canónica y sincroniza `channel_registry`.
- El usuario no necesita escribir `channel_id`, `uploads_playlist_id` ni registrar canales manualmente.
- Si una URL deja de estar en `CHANNEL_URLS`, el registry la marca como inactiva y deja de procesarla.

## Flujo diario actual

1. Leer y normalizar `CHANNEL_URLS`.
2. Sincronizar `channel_registry` automáticamente.
3. Procesar todos los canales activos y resueltos del registry.
4. Descubrir vídeos recientes desde la uploads playlist de cada canal.
5. Enriquecer cada lote con `videos.list` para mantener todas las columnas de `EXPORT_COLUMNS`.
6. Reemplazar completamente la worksheet `videos_current_60d`.
7. Opcionalmente hacer append en `videos_history` cuando `WRITE_HISTORY=true`.
8. Generar reportes operativos (`run_report.json`, `unresolved_urls.json`, `channel_failures.json`).

## Outputs

- `videos_current_60d`: snapshot actual de la ventana configurada (por defecto 60 días).
- `videos_history`: histórico append-only opcional, controlado por `WRITE_HISTORY`.
- `channel_registry`: caché automática de resolución de canales.
- `run_report.json`: resumen técnico de la ejecución más reciente.
- `unresolved_urls.json`: lista de URLs no resueltas en la corrida.
- `channel_failures.json`: canales que fallaron parcialmente sin tumbar la ejecución completa.

## Operación desde GitHub Actions

El repositorio está pensado para operarse solo desde GitHub Actions.

El workflow `.github/workflows/youtube_data.yml` se ejecuta diariamente y también puede lanzarse manualmente con `workflow_dispatch`.

Inputs opcionales del dispatch:

- `days`
- `channel_limit`
- `channel_filter`
- `force_refresh_registry`
- `write_history`

Secretos requeridos:

- `YOUTUBE_API_KEY`
- `GOOGLE_SHEETS_CREDS_BASE64`
- `SPREADSHEET_ID`

Artifacts que deja cada ejecución:

- `youtube_data.log`
- `run_report.json`
- `unresolved_urls.json`
- `channel_failures.json`

## Requisitos

- Python 3.12
- `YOUTUBE_API_KEY`
- `GOOGLE_SHEETS_CREDS_BASE64`
- `SPREADSHEET_ID`

## Desarrollo y tests

Instalación de dependencias:

```bash
pip install -r requirements.txt
```

Ejecución de tests:

```bash
python -m unittest discover -s tests -p 'test_*.py' -v
```

La CI de `.github/workflows/ci.yml` ejecuta exactamente esa suite de tests en Python 3.12.
