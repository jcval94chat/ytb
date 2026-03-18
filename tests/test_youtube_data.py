import base64
import json
import os
import unittest
from unittest.mock import ANY, MagicMock, mock_open, patch

import pandas as pd

import youtube_data


class YoutubeDataTests(unittest.TestCase):
    def make_channel_item(self, channel_id='UC123', title='Canal', uploads='UU123'):
        return {
            'id': channel_id,
            'snippet': {'title': title},
            'contentDetails': {'relatedPlaylists': {'uploads': uploads}},
        }

    def make_video_item(self, video_id='vid-1', published_at='2026-03-10T08:30:00Z'):
        return {
            'id': video_id,
            'snippet': {
                'title': f'Title {video_id}',
                'description': 'Description',
                'publishedAt': published_at,
                'tags': ['tag1', 'tag2'],
                'thumbnails': {'high': {'url': f'https://i.ytimg.com/vi/{video_id}/hqdefault.jpg'}},
                'categoryId': '28',
                'defaultLanguage': 'en',
                'defaultAudioLanguage': 'en-US',
                'liveBroadcastContent': 'none',
            },
            'contentDetails': {
                'duration': 'PT1M5S',
                'dimension': '2d',
                'definition': 'hd',
                'caption': 'true',
                'licensedContent': True,
                'projection': 'rectangular',
            },
            'statistics': {
                'viewCount': '100',
                'likeCount': '10',
                'commentCount': '2',
            },
            'status': {
                'privacyStatus': 'public',
                'license': 'youtube',
                'embeddable': True,
                'publicStatsViewable': True,
                'madeForKids': False,
                'selfDeclaredMadeForKids': False,
            },
        }

    def test_build_video_record_includes_new_exportable_columns(self):
        execution_time = pd.Timestamp('2026-03-18T12:00:00Z').to_pydatetime()
        channel_context = {
            'channel_name': 'Veritasium',
            'channel_id': 'UCtest123',
            'source_channel_url': 'https://www.youtube.com/@Veritasium',
            'channel_custom_url': '@Veritasium',
            'channel_country': 'US',
            'channel_published_at': '2011-01-01T00:00:00Z',
            'subscriber_count_snapshot': '15200000',
            'channel_total_views_snapshot': '3123456789',
            'channel_video_count_snapshot': '421',
        }

        record = youtube_data.build_video_record(
            self.make_video_item(video_id='abc123XYZ'),
            channel_context,
            execution_time=execution_time,
        )

        self.assertEqual(record['video_id'], 'abc123XYZ')
        self.assertEqual(record['video_url'], 'https://www.youtube.com/watch?v=abc123XYZ')
        self.assertEqual(record['duration_iso'], 'PT1M5S')
        self.assertEqual(record['duration_seconds'], '65')
        self.assertEqual(record['engagement_rate'], '0.120000')
        self.assertEqual(record['licensed_content'], 'true')
        self.assertEqual(record['subscriber_count_snapshot'], '15200000')
        self.assertEqual(list(record.keys()), youtube_data.EXPORT_COLUMNS)

    def test_prepare_dataframe_for_export_conserves_export_columns(self):
        raw_df = pd.DataFrame([
            {
                'channel_name': 'Canal X',
                'channel_id': 'UCX',
                'video_id': 'vid1',
                'views': 100,
                'licensed_content': True,
                'comments': None,
                'engagement_rate': 0.1,
            }
        ])

        export_df = youtube_data.prepare_dataframe_for_export(raw_df)

        self.assertEqual(list(export_df.columns), youtube_data.EXPORT_COLUMNS)
        self.assertTrue(all(dtype == object for dtype in export_df.dtypes))
        self.assertEqual(export_df.loc[0, 'views'], '100')
        self.assertEqual(export_df.loc[0, 'licensed_content'], 'true')
        self.assertEqual(export_df.loc[0, 'comments'], '')
        self.assertEqual(export_df.loc[0, 'engagement_rate'], '0.1')

    def test_ensure_sheet_capacity_resizes_when_needed(self):
        sheet = MagicMock()
        sheet.title = 'Videos'
        sheet.row_count = 10
        sheet.col_count = 5

        youtube_data.ensure_sheet_capacity(sheet, required_rows=25, required_cols=12)

        sheet.resize.assert_called_once_with(rows=25, cols=12)

    def test_write_replace_sheet_replaces_content(self):
        sheet = MagicMock()
        sheet.title = 'videos_current_60d'
        sheet.row_count = 100
        sheet.col_count = 100
        df = pd.DataFrame([
            {
                'channel_name': 'Canal X',
                'channel_id': 'UCX',
                'video_id': 'vid1',
                'video_url': 'https://www.youtube.com/watch?v=vid1',
            }
        ])

        youtube_data.write_replace_sheet(sheet, df)

        sheet.clear.assert_called_once()
        sheet.update.assert_called_once()
        update_payload = sheet.update.call_args.args[0]
        self.assertEqual(update_payload[0], youtube_data.EXPORT_COLUMNS)
        self.assertEqual(update_payload[1][0], 'Canal X')
        self.assertEqual(update_payload[1][9], 'vid1')

    def test_append_videos_history_is_append_only_and_deduplicates_video_execution_key(self):
        spreadsheet = MagicMock()
        history_sheet = MagicMock()
        history_sheet.title = youtube_data.VIDEOS_HISTORY_SHEET
        history_sheet.get_all_records.return_value = [
            {'video_id': 'vid-1', 'execution_date': '2026-03-18'}
        ]
        df = pd.DataFrame([
            {'channel_name': 'Canal', 'channel_id': 'UC1', 'video_id': 'vid-1', 'execution_date': '2026-03-18'},
            {'channel_name': 'Canal', 'channel_id': 'UC1', 'video_id': 'vid-2', 'execution_date': '2026-03-18'},
        ])

        with patch.object(youtube_data, 'get_or_create_worksheet', return_value=history_sheet), \
             patch.object(youtube_data, 'append_sheet_rows') as mock_append_rows:
            appended_rows = youtube_data.append_videos_history(spreadsheet, df)

        self.assertEqual(appended_rows, 1)
        appended_df = mock_append_rows.call_args.args[1]
        self.assertEqual(appended_df['video_id'].tolist(), ['vid-2'])

    def test_fetch_channel_context_reads_channel_statistics(self):
        youtube = MagicMock()
        youtube.channels.return_value.list.return_value.execute.return_value = {
            'items': [
                {
                    'snippet': {
                        'customUrl': '@Veritasium',
                        'country': 'US',
                        'publishedAt': '2011-01-01T00:00:00Z',
                    },
                    'statistics': {
                        'subscriberCount': '15200000',
                        'viewCount': '3123456789',
                        'videoCount': '421',
                    },
                }
            ]
        }
        run_report = youtube_data.create_run_report('2026-03-18T00:00:00Z')

        context = youtube_data.fetch_channel_context(
            youtube,
            channel_id='UCtest123',
            channel_name='Veritasium',
            channel_url='https://www.youtube.com/@Veritasium',
            run_report=run_report,
        )

        self.assertEqual(run_report['api_calls']['channels_list'], 1)
        self.assertEqual(context['channel_total_views_snapshot'], '3123456789')

    def test_resolve_channel_url_uses_for_handle_for_handles(self):
        youtube = MagicMock()
        youtube.channels.return_value.list.return_value.execute.return_value = {
            'items': [self.make_channel_item(channel_id='UC_HANDLE', title='Handle Channel', uploads='UU_HANDLE')]
        }

        row = youtube_data.resolve_channel_url(youtube, 'https://www.youtube.com/@handle')

        kwargs = youtube.channels.return_value.list.call_args.kwargs
        self.assertEqual(kwargs['forHandle'], 'handle')
        self.assertIsNone(kwargs['forUsername'])
        self.assertEqual(row['channel_id'], 'UC_HANDLE')
        self.assertEqual(row['resolver_type'], 'handle')

    def test_resolve_channel_url_uses_for_username_for_user_urls(self):
        youtube = MagicMock()
        youtube.channels.return_value.list.return_value.execute.return_value = {
            'items': [self.make_channel_item(channel_id='UC_USER', title='User Channel', uploads='UU_USER')]
        }

        row = youtube_data.resolve_channel_url(youtube, 'https://www.youtube.com/user/testuser')

        kwargs = youtube.channels.return_value.list.call_args.kwargs
        self.assertEqual(kwargs['forUsername'], 'testuser')
        self.assertIsNone(kwargs['forHandle'])
        self.assertEqual(row['channel_id'], 'UC_USER')
        self.assertEqual(row['resolver_type'], 'username')

    def test_resolve_channel_url_uses_direct_channel_id_for_channel_urls(self):
        youtube = MagicMock()
        youtube.channels.return_value.list.return_value.execute.return_value = {
            'items': [self.make_channel_item(channel_id='UC_DIRECT', title='Direct Channel', uploads='UU_DIRECT')]
        }

        row = youtube_data.resolve_channel_url(youtube, 'https://www.youtube.com/channel/UC_DIRECT')

        kwargs = youtube.channels.return_value.list.call_args.kwargs
        self.assertEqual(kwargs['id'], 'UC_DIRECT')
        self.assertEqual(row['channel_id'], 'UC_DIRECT')
        self.assertEqual(row['resolver_type'], 'channel_id')

    @patch('youtube_data.requests.get')
    def test_resolve_channel_url_follows_redirect_for_c_urls(self, mock_requests_get):
        youtube = MagicMock()
        youtube.channels.return_value.list.return_value.execute.return_value = {
            'items': [self.make_channel_item(channel_id='UC_C', title='Canonical Channel', uploads='UU_C')]
        }
        response = MagicMock()
        response.url = 'https://www.youtube.com/@canonical'
        mock_requests_get.return_value = response

        row = youtube_data.resolve_channel_url(youtube, 'https://www.youtube.com/c/canonical')

        mock_requests_get.assert_called_once()
        kwargs = youtube.channels.return_value.list.call_args.kwargs
        self.assertEqual(kwargs['forHandle'], 'canonical')
        self.assertEqual(row['resolver_type'], 'redirect_handle')
        self.assertEqual(row['normalized_url'], 'https://www.youtube.com/@canonical')

    def test_sync_channel_registry_deduplicates_same_channel_id(self):
        youtube = MagicMock()
        spreadsheet = MagicMock()

        with patch.object(youtube_data, 'load_channel_registry', return_value=pd.DataFrame(columns=youtube_data.CHANNEL_REGISTRY_COLUMNS)), \
             patch.object(
                 youtube_data,
                 'resolve_channel_url',
                 side_effect=[
                     {
                         'source_url': 'https://www.youtube.com/@alpha',
                         'normalized_url': 'https://www.youtube.com/@alpha',
                         'channel_id': 'UC_DUP',
                         'uploads_playlist_id': 'UU_DUP',
                         'channel_name': 'Canal Duplicado',
                         'resolver_type': 'handle',
                         'active': 'true',
                         'resolution_status': 'resolved',
                         'last_verified_at': '2026-03-18T00:00:00Z',
                         'last_error': '',
                     },
                     {
                         'source_url': 'https://www.youtube.com/channel/UC_DUP',
                         'normalized_url': 'https://www.youtube.com/channel/UC_DUP',
                         'channel_id': 'UC_DUP',
                         'uploads_playlist_id': 'UU_DUP',
                         'channel_name': 'Canal Duplicado',
                         'resolver_type': 'channel_id',
                         'active': 'true',
                         'resolution_status': 'resolved',
                         'last_verified_at': '2026-03-18T00:00:01Z',
                         'last_error': '',
                     },
                 ],
             ), \
             patch.object(youtube_data, 'write_channel_registry'):
            registry_df = youtube_data.sync_channel_registry_from_urls(
                youtube,
                spreadsheet,
                ['https://www.youtube.com/@alpha', 'https://www.youtube.com/channel/UC_DUP'],
            )

        self.assertEqual(registry_df['channel_id'].tolist(), ['UC_DUP'])

    @patch('youtube_data.requests.get')
    def test_resolve_channel_url_records_error_for_invalid_url(self, mock_requests_get):
        youtube = MagicMock()
        youtube.channels.return_value.list.return_value.execute.return_value = {'items': []}
        youtube.search.return_value.list.return_value.execute.return_value = {'items': []}
        response = MagicMock()
        response.url = 'https://www.youtube.com/invalid/path'
        mock_requests_get.return_value = response

        row = youtube_data.resolve_channel_url(youtube, 'https://www.youtube.com/invalid/path')

        self.assertEqual(row['resolution_status'], 'error')
        self.assertIn('No se pudo resolver', row['last_error'])

    def test_list_recent_video_ids_from_uploads_uses_playlistitems_for_discovery(self):
        youtube = MagicMock()
        youtube.playlistItems.return_value.list.return_value.execute.return_value = {
            'items': [
                {
                    'contentDetails': {
                        'videoId': 'vid-1',
                        'videoPublishedAt': '2026-03-15T00:00:00Z',
                    },
                    'snippet': {'publishedAt': '2026-03-15T00:00:00Z'},
                },
                {
                    'contentDetails': {
                        'videoId': 'vid-2',
                        'videoPublishedAt': '2026-01-01T00:00:00Z',
                    },
                    'snippet': {'publishedAt': '2026-01-01T00:00:00Z'},
                },
            ]
        }
        run_report = youtube_data.create_run_report('2026-03-18T00:00:00Z')

        with patch('youtube_data.utc_now', return_value=pd.Timestamp('2026-03-18T00:00:00Z').to_pydatetime()):
            video_ids = youtube_data.list_recent_video_ids_from_uploads(
                youtube,
                uploads_playlist_id='UU123',
                days=60,
                run_report=run_report,
            )

        youtube.playlistItems.return_value.list.assert_called_once()
        self.assertEqual(video_ids, ['vid-1'])
        self.assertEqual(run_report['api_calls']['playlistitems_list'], 1)
        self.assertEqual(run_report['videos_discovered'], 1)

    def test_fetch_video_details_batch_uses_videos_list_for_enrichment(self):
        youtube = MagicMock()
        youtube.videos.return_value.list.return_value.execute.return_value = {
            'items': [self.make_video_item('vid-1')]
        }
        run_report = youtube_data.create_run_report('2026-03-18T00:00:00Z')

        items = youtube_data.fetch_video_details_batch(youtube, ['vid-1', 'vid-2'], run_report=run_report)

        kwargs = youtube.videos.return_value.list.call_args.kwargs
        self.assertEqual(kwargs['id'], 'vid-1,vid-2')
        self.assertEqual(run_report['api_calls']['videos_list'], 1)
        self.assertEqual(len(items), 1)

    def test_get_channel_videos_uses_playlistitems_and_videos_not_search(self):
        youtube = MagicMock()
        youtube.channels.return_value.list.return_value.execute.return_value = {
            'items': [
                {
                    'snippet': {'customUrl': '@canal', 'country': 'ES', 'publishedAt': '2011-01-01T00:00:00Z'},
                    'statistics': {'subscriberCount': '10', 'viewCount': '20', 'videoCount': '30'},
                }
            ]
        }
        youtube.playlistItems.return_value.list.return_value.execute.return_value = {
            'items': [
                {
                    'contentDetails': {'videoId': 'vid-1', 'videoPublishedAt': '2026-03-15T00:00:00Z'},
                    'snippet': {'publishedAt': '2026-03-15T00:00:00Z'},
                }
            ]
        }
        youtube.videos.return_value.list.return_value.execute.return_value = {
            'items': [self.make_video_item('vid-1')]
        }
        run_report = youtube_data.create_run_report('2026-03-18T00:00:00Z')

        with patch('youtube_data.utc_now', return_value=pd.Timestamp('2026-03-18T12:00:00Z').to_pydatetime()):
            df = youtube_data.get_channel_videos(
                youtube,
                channel_id='UC123',
                channel_name='Canal',
                channel_url='https://www.youtube.com/@canal',
                uploads_playlist_id='UU123',
                days=60,
                run_report=run_report,
            )

        youtube.playlistItems.return_value.list.assert_called_once()
        youtube.videos.return_value.list.assert_called_once()
        youtube.search.return_value.list.assert_not_called()
        self.assertEqual(df['video_id'].tolist(), ['vid-1'])
        self.assertEqual(list(df.columns), youtube_data.EXPORT_COLUMNS)

    @patch('youtube_data.write_operational_artifacts')
    @patch('youtube_data.append_videos_history')
    @patch('youtube_data.write_replace_sheet')
    @patch('youtube_data.get_channel_videos')
    @patch('youtube_data.sync_channel_registry_from_urls')
    @patch('youtube_data.get_urls')
    @patch('youtube_data.build')
    @patch('youtube_data.gspread.authorize')
    @patch('youtube_data.Credentials.from_service_account_info')
    def test_main_replaces_snapshot_and_optionally_appends_history(
        self,
        mock_credentials,
        mock_authorize,
        mock_build,
        mock_get_urls,
        mock_sync_registry,
        mock_get_channel_videos,
        mock_write_replace_sheet,
        mock_append_history,
        mock_write_operational_artifacts,
    ):
        mock_credentials.return_value = object()
        creds_payload = base64.b64encode(json.dumps({'type': 'service_account'}).encode()).decode()
        current_sheet = MagicMock()
        current_sheet.title = youtube_data.VIDEOS_CURRENT_SHEET
        registry_sheet = MagicMock()
        registry_sheet.title = youtube_data.CHANNEL_REGISTRY_SHEET
        history_sheet = MagicMock()
        history_sheet.title = youtube_data.VIDEOS_HISTORY_SHEET
        spreadsheet = MagicMock()
        spreadsheet.title = 'YT Data'
        spreadsheet.worksheet.side_effect = [current_sheet, registry_sheet, history_sheet]
        client = MagicMock()
        client.open_by_key.return_value = spreadsheet
        mock_authorize.return_value = client
        youtube_client = MagicMock()
        mock_build.return_value = youtube_client
        mock_get_urls.return_value = ['https://www.youtube.com/@canal']
        registry_df = pd.DataFrame([
            {
                'source_url': 'https://www.youtube.com/@canal',
                'normalized_url': 'https://www.youtube.com/@canal',
                'channel_id': 'UC123',
                'uploads_playlist_id': 'UU123',
                'channel_name': 'Canal',
                'resolver_type': 'handle',
                'active': 'true',
                'resolution_status': 'resolved',
                'last_verified_at': '2026-03-18T00:00:00Z',
                'last_error': '',
            }
        ])
        mock_sync_registry.return_value = registry_df
        channel_df = pd.DataFrame([
            {
                'channel_name': 'Canal',
                'channel_id': 'UC123',
                'video_id': 'new-video',
                'video_url': 'https://www.youtube.com/watch?v=new-video',
                'execution_date': '2026-03-18',
            }
        ])
        channel_df.attrs['channel_fetch_failed'] = False
        mock_get_channel_videos.return_value = channel_df
        mock_append_history.return_value = 1

        with patch.dict(
            os.environ,
            {
                'YOUTUBE_API_KEY': 'api-key',
                'GOOGLE_SHEETS_CREDS_BASE64': creds_payload,
                'SPREADSHEET_ID': 'spreadsheet-id',
                'WRITE_HISTORY': 'true',
                'GITHUB_EVENT_NAME': 'workflow_dispatch',
            },
            clear=True,
        ):
            result = youtube_data.main()

        self.assertEqual(result, 0)
        mock_write_replace_sheet.assert_called_once()
        exported_df = mock_write_replace_sheet.call_args.args[1]
        self.assertEqual(exported_df['video_id'].tolist(), ['new-video'])
        mock_append_history.assert_called_once_with(spreadsheet, exported_df)
        run_report = mock_write_operational_artifacts.call_args.args[0]
        self.assertEqual(run_report['history_rows_appended'], 1)
        self.assertEqual(run_report['write_history_enabled'], 'true')
        self.assertEqual(run_report['execution_mode'], 'manual')

    @patch('youtube_data.write_operational_artifacts')
    @patch('youtube_data.write_replace_sheet')
    @patch('youtube_data.get_channel_videos')
    @patch('youtube_data.sync_channel_registry_from_urls')
    @patch('youtube_data.get_urls')
    @patch('youtube_data.build')
    @patch('youtube_data.gspread.authorize')
    @patch('youtube_data.Credentials.from_service_account_info')
    def test_partial_channel_errors_do_not_fail_entire_run(
        self,
        mock_credentials,
        mock_authorize,
        mock_build,
        mock_get_urls,
        mock_sync_registry,
        mock_get_channel_videos,
        mock_write_replace_sheet,
        mock_write_operational_artifacts,
    ):
        mock_credentials.return_value = object()
        creds_payload = base64.b64encode(json.dumps({'type': 'service_account'}).encode()).decode()
        current_sheet = MagicMock()
        current_sheet.title = youtube_data.VIDEOS_CURRENT_SHEET
        registry_sheet = MagicMock()
        registry_sheet.title = youtube_data.CHANNEL_REGISTRY_SHEET
        spreadsheet = MagicMock()
        spreadsheet.title = 'YT Data'
        spreadsheet.worksheet.side_effect = [current_sheet, registry_sheet]
        client = MagicMock()
        client.open_by_key.return_value = spreadsheet
        mock_authorize.return_value = client
        mock_build.return_value = MagicMock()
        mock_get_urls.return_value = ['https://www.youtube.com/@ok', 'https://www.youtube.com/@fail']
        registry_df = pd.DataFrame([
            {
                'source_url': 'https://www.youtube.com/@ok',
                'normalized_url': 'https://www.youtube.com/@ok',
                'channel_id': 'UC_OK',
                'uploads_playlist_id': 'UU_OK',
                'channel_name': 'OK',
                'resolver_type': 'handle',
                'active': 'true',
                'resolution_status': 'resolved',
                'last_verified_at': '2026-03-18T00:00:00Z',
                'last_error': '',
            },
            {
                'source_url': 'https://www.youtube.com/@fail',
                'normalized_url': 'https://www.youtube.com/@fail',
                'channel_id': 'UC_FAIL',
                'uploads_playlist_id': 'UU_FAIL',
                'channel_name': 'FAIL',
                'resolver_type': 'handle',
                'active': 'true',
                'resolution_status': 'resolved',
                'last_verified_at': '2026-03-18T00:00:00Z',
                'last_error': '',
            },
        ])
        mock_sync_registry.return_value = registry_df
        ok_df = pd.DataFrame([
            {'channel_name': 'OK', 'channel_id': 'UC_OK', 'video_id': 'vid-ok', 'video_url': 'https://www.youtube.com/watch?v=vid-ok'}
        ])
        ok_df.attrs['channel_fetch_failed'] = False
        fail_df = pd.DataFrame(columns=youtube_data.EXPORT_COLUMNS)
        fail_df.attrs['channel_fetch_failed'] = True
        mock_get_channel_videos.side_effect = [ok_df, fail_df]

        with patch.dict(
            os.environ,
            {
                'YOUTUBE_API_KEY': 'api-key',
                'GOOGLE_SHEETS_CREDS_BASE64': creds_payload,
                'SPREADSHEET_ID': 'spreadsheet-id',
            },
            clear=True,
        ):
            result = youtube_data.main()

        self.assertEqual(result, 0)
        exported_df = mock_write_replace_sheet.call_args.args[1]
        self.assertEqual(exported_df['video_id'].tolist(), ['vid-ok'])
        run_report = mock_write_operational_artifacts.call_args.args[0]
        self.assertEqual(run_report['channels_processed'], 1)
        self.assertEqual(run_report['channels_failed'], 1)
        self.assertEqual(len(run_report['channel_failures']), 1)

    @patch('youtube_data.write_operational_artifacts')
    @patch('youtube_data.write_replace_sheet')
    @patch('youtube_data.get_channel_videos')
    @patch('youtube_data.sync_channel_registry_from_urls')
    @patch('youtube_data.get_urls')
    @patch('youtube_data.build')
    @patch('youtube_data.gspread.authorize')
    @patch('youtube_data.Credentials.from_service_account_info')
    def test_run_report_contains_expected_operational_fields(
        self,
        mock_credentials,
        mock_authorize,
        mock_build,
        mock_get_urls,
        mock_sync_registry,
        mock_get_channel_videos,
        mock_write_replace_sheet,
        mock_write_operational_artifacts,
    ):
        mock_credentials.return_value = object()
        creds_payload = base64.b64encode(json.dumps({'type': 'service_account'}).encode()).decode()
        current_sheet = MagicMock()
        current_sheet.title = youtube_data.VIDEOS_CURRENT_SHEET
        registry_sheet = MagicMock()
        registry_sheet.title = youtube_data.CHANNEL_REGISTRY_SHEET
        spreadsheet = MagicMock()
        spreadsheet.title = 'YT Data'
        spreadsheet.worksheet.side_effect = [current_sheet, registry_sheet]
        client = MagicMock()
        client.open_by_key.return_value = spreadsheet
        mock_authorize.return_value = client
        mock_build.return_value = MagicMock()
        mock_get_urls.return_value = ['https://www.youtube.com/@canal']
        registry_df = pd.DataFrame([
            {
                'source_url': 'https://www.youtube.com/@canal',
                'normalized_url': 'https://www.youtube.com/@canal',
                'channel_id': 'UC123',
                'uploads_playlist_id': 'UU123',
                'channel_name': 'Canal',
                'resolver_type': 'handle',
                'active': 'true',
                'resolution_status': 'resolved',
                'last_verified_at': '2026-03-18T00:00:00Z',
                'last_error': '',
            },
            {
                'source_url': 'https://www.youtube.com/@broken',
                'normalized_url': 'https://www.youtube.com/@broken',
                'channel_id': '',
                'uploads_playlist_id': '',
                'channel_name': '',
                'resolver_type': 'handle',
                'active': 'true',
                'resolution_status': 'error',
                'last_verified_at': '2026-03-18T00:00:00Z',
                'last_error': 'boom',
            },
        ])
        mock_sync_registry.return_value = registry_df
        channel_df = pd.DataFrame([
            {'channel_name': 'Canal', 'channel_id': 'UC123', 'video_id': 'vid-1', 'video_url': 'https://www.youtube.com/watch?v=vid-1'}
        ])
        channel_df.attrs['channel_fetch_failed'] = False
        mock_get_channel_videos.return_value = channel_df

        with patch.dict(
            os.environ,
            {
                'YOUTUBE_API_KEY': 'api-key',
                'GOOGLE_SHEETS_CREDS_BASE64': creds_payload,
                'SPREADSHEET_ID': 'spreadsheet-id',
                'DAYS': '15',
                'CHANNEL_LIMIT': '2',
                'CHANNEL_FILTER': 'canal',
                'GITHUB_EVENT_NAME': 'schedule',
            },
            clear=True,
        ):
            result = youtube_data.main()

        self.assertEqual(result, 0)
        run_report = mock_write_operational_artifacts.call_args.args[0]
        expected_fields = {
            'registry_rows_total',
            'registry_rows_active',
            'registry_rows_failed',
            'new_channels_resolved_this_run',
            'videos_discovered',
            'videos_exported',
            'execution_mode',
            'days_window',
            'channel_limit_applied',
            'channel_filter_applied',
            'channel_failures',
            'unresolved_urls',
        }
        self.assertTrue(expected_fields.issubset(run_report.keys()))
        self.assertEqual(run_report['execution_mode'], 'scheduled')
        self.assertEqual(run_report['days_window'], 15)
        self.assertEqual(run_report['channel_limit_applied'], '2')
        self.assertEqual(run_report['channel_filter_applied'], 'canal')
        self.assertEqual(run_report['registry_rows_total'], 2)
        self.assertEqual(run_report['registry_rows_active'], 2)
        self.assertEqual(run_report['registry_rows_failed'], 1)
        self.assertEqual(run_report['videos_exported'], 1)
        self.assertEqual(run_report['unresolved_urls'], ['https://www.youtube.com/@broken'])

    def test_load_channel_registry_uses_internal_worksheet_schema(self):
        sheet = MagicMock()
        sheet.get_all_records.return_value = [
            {
                'source_url': 'https://www.youtube.com/@canal',
                'normalized_url': 'https://www.youtube.com/@canal',
                'channel_id': 'UC123',
                'uploads_playlist_id': 'UU123',
                'channel_name': 'Canal',
                'resolver_type': 'handle',
                'active': True,
                'resolution_status': 'resolved',
                'last_verified_at': '2026-03-18T00:00:00Z',
                'last_error': '',
            }
        ]
        spreadsheet = MagicMock()

        with patch.object(youtube_data, 'get_or_create_worksheet', return_value=sheet):
            registry_df = youtube_data.load_channel_registry(spreadsheet)

        self.assertEqual(list(registry_df.columns), youtube_data.CHANNEL_REGISTRY_COLUMNS)
        self.assertEqual(registry_df.loc[0, 'active'], 'true')


if __name__ == '__main__':
    unittest.main()
