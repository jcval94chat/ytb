import unittest
from unittest.mock import patch

import get_urls


class GetUrlsTests(unittest.TestCase):
    def test_normalize_channel_url_strips_handle_videos_suffix(self):
        normalized = get_urls.normalize_channel_url(' https://m.youtube.com/@handle/videos?view=0#section ')

        self.assertEqual(normalized, 'https://www.youtube.com/@handle')

    def test_normalize_channel_url_preserves_handle_path(self):
        normalized = get_urls.normalize_channel_url('https://www.youtube.com/@handle')

        self.assertEqual(normalized, 'https://www.youtube.com/@handle')

    def test_get_urls_removes_duplicates_and_preserves_order(self):
        channel_urls = [
            'https://www.youtube.com/@alpha/videos',
            'https://www.youtube.com/@alpha',
            'https://m.youtube.com/@beta/',
            'https://www.youtube.com/@beta/community?view=0',
            'https://www.youtube.com/@gamma',
        ]

        with patch.object(get_urls, 'CHANNEL_URLS', channel_urls):
            urls = get_urls.get_urls()

        self.assertEqual(
            urls,
            [
                'https://www.youtube.com/@alpha',
                'https://www.youtube.com/@beta',
                'https://www.youtube.com/@gamma',
            ],
        )


if __name__ == '__main__':
    unittest.main()
