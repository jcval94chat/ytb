import json
import unittest
from unittest.mock import MagicMock

import pandas as pd

import youtube_data


class YoutubeDataTests(unittest.TestCase):
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
        item = {
            'id': 'abc123XYZ',
            'snippet': {
                'title': 'Why Airplanes Fly',
                'description': 'Physics of lift.',
                'publishedAt': '2026-03-10T08:30:00Z',
                'tags': ['physics', 'aviation'],
                'thumbnails': {'high': {'url': 'https://i.ytimg.com/vi/abc123XYZ/hqdefault.jpg'}},
                'categoryId': '28',
                'defaultLanguage': 'en',
                'defaultAudioLanguage': 'en-US',
                'liveBroadcastContent': 'none',
            },
            'contentDetails': {
                'duration': 'PT14M5S',
                'dimension': '2d',
                'definition': 'hd',
                'caption': 'true',
                'licensedContent': True,
                'projection': 'rectangular',
            },
            'statistics': {
                'viewCount': '154320',
                'likeCount': '9800',
                'commentCount': '742',
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

        record = youtube_data.build_video_record(item, channel_context, execution_time=execution_time)

        self.assertEqual(record['video_id'], 'abc123XYZ')
        self.assertEqual(record['video_url'], 'https://www.youtube.com/watch?v=abc123XYZ')
        self.assertEqual(record['duration_iso'], 'PT14M5S')
        self.assertEqual(record['duration_seconds'], '845')
        self.assertEqual(record['engagement_rate'], '0.068313')
        self.assertEqual(record['days_since_upload'], '8')
        self.assertEqual(record['licensed_content'], 'true')
        self.assertEqual(record['embeddable'], 'true')
        self.assertEqual(record['made_for_kids'], 'false')
        self.assertEqual(record['subscriber_count_snapshot'], '15200000')
        self.assertEqual(record['fetched_at'], '2026-03-18T12:00:00Z')
        self.assertEqual(list(record.keys()), youtube_data.EXPORT_COLUMNS)

    def test_prepare_dataframe_for_export_converts_everything_to_text_and_keeps_order(self):
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

    def test_export_dataframe_to_sheet_writes_headers_and_rows(self):
        sheet = MagicMock()
        sheet.title = 'Videos'
        sheet.row_count = 100
        sheet.col_count = 100
        combined_df = pd.DataFrame([
            {
                'channel_name': 'Canal X',
                'channel_id': 'UCX',
                'video_id': 'vid1',
                'video_url': 'https://www.youtube.com/watch?v=vid1',
            }
        ])

        youtube_data.export_dataframe_to_sheet(sheet, combined_df)

        sheet.clear.assert_called_once()
        sheet.update.assert_called_once()
        update_payload = sheet.update.call_args.args[0]
        self.assertEqual(update_payload[0], youtube_data.EXPORT_COLUMNS)
        self.assertEqual(update_payload[1][0], 'Canal X')
        self.assertEqual(update_payload[1][1], 'UCX')
        self.assertEqual(update_payload[1][9], 'vid1')

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

        context = youtube_data.fetch_channel_context(
            youtube,
            channel_id='UCtest123',
            channel_name='Veritasium',
            channel_url='https://www.youtube.com/@Veritasium',
        )

        self.assertEqual(
            context,
            {
                'channel_name': 'Veritasium',
                'channel_id': 'UCtest123',
                'source_channel_url': 'https://www.youtube.com/@Veritasium',
                'channel_custom_url': '@Veritasium',
                'channel_country': 'US',
                'channel_published_at': '2011-01-01T00:00:00Z',
                'subscriber_count_snapshot': '15200000',
                'channel_total_views_snapshot': '3123456789',
                'channel_video_count_snapshot': '421',
            },
        )


if __name__ == '__main__':
    unittest.main()
