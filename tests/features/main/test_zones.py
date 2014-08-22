import unittest
from moto import mock_s3
import boto
import tempfile
import shutil
import gzip

from cdf.features.main.zones import (
    get_lang,
    compute_zones,
    generate_zone_stream
)
from cdf.features.main.streams import ZoneStreamDef


class TestGetLang(unittest.TestCase):
    def setUp(self):
        self.info_entry = [0, "foo", "en-US", "?"]

    def test_nominal_case(self):
        self.assertEqual("en-US", get_lang(self.info_entry, 2))

    def test_lang_not_set(self):
        self.assertEqual("notset", get_lang(self.info_entry, 4))

    def test_lang_unknown(self):
        self.assertEqual("notset", get_lang(self.info_entry, 3))


class TestGenerateZoneStream(unittest.TestCase):
    def test_nominal_case(self):
        id_stream = iter([
            (1, "http", "foo.com", "/"),
            (2, "https", "foo.com", "/bar"),
            (9, "https", "foo.com", "/baz"),
        ])

        infos_stream = iter([
            (1, None, None, None, None, None, None, None, None, "en-US"),
            (2, None, None, None, None, None, None, None, None, "fr"),
            (9, None, None, None, None, None, None, None, None, "?"),
        ])

        zone_stream = generate_zone_stream(id_stream, infos_stream)

        expected_stream = [
            (1, 'en-US,http'),
            (2, 'fr,https'),
            (9, 'notset,https')
        ]
        self.assertEqual(expected_stream, list(zone_stream))


class TestComputeZones(unittest.TestCase):
    def setUp(self):
        self.bucket_name = "app.foo.com"
        self.s3_uri = "s3://{}/crawl_result".format(self.bucket_name)

        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @mock_s3
    def test_nominal_case(self):

        conn = boto.connect_s3()
        bucket = conn.create_bucket(self.bucket_name)

        #create urlids
        urlids = boto.s3.key.Key(bucket)
        urlids.key = "crawl_result/urlids.txt.0.gz"
        f = tempfile.NamedTemporaryFile(delete=False, prefix=self.tmp_dir)
        f.close()
        with gzip.open(f.name, "w") as tmp_file:
            tmp_file.write("1\thttp\tfoo.com\t/\n")
            tmp_file.write("2\thttps\tfoo.com\t/bar\n")
            tmp_file.write("9\thttps\tfoo.com\t/baz\n")
        urlids.set_contents_from_filename(f.name)

        #create urlinfos
        urlinfos = boto.s3.key.Key(bucket)
        urlinfos.key = "crawl_result/urlinfos.txt.0.gz"
        bulk_data = "48\ttext/html\t0\t7695895\t200\t51133\t203\t305"
        f = tempfile.NamedTemporaryFile(delete=False, prefix=self.tmp_dir)
        f.close()
        with gzip.open(f.name, "w") as tmp_file:
            tmp_file.write("1\t{}\ten-US\n".format(bulk_data))
            tmp_file.write("2\t{}\tfr\n".format(bulk_data))
            tmp_file.write("9\t{}\tfr\n".format(bulk_data))
        urlinfos.set_contents_from_filename(f.name)

        #actual computation
        part_id = 0
        document_uri = compute_zones(self.s3_uri,
                                     part_id)

        #check output
        expected_document_uri = "{}/zones.txt.{}.gz".format(self.s3_uri, part_id)
        self.assertEqual(expected_document_uri, document_uri)

        zone_stream = ZoneStreamDef.get_stream_from_s3(self.s3_uri,
                                                       tmp_dir=self.tmp_dir,
                                                       part_id=part_id)
        expected_zone_stream = [
            [1, 'en-US,http'],
            [2, 'fr,https'],
            [9, 'fr,https']
        ]
        self.assertEqual(expected_zone_stream, list(zone_stream))

    @mock_s3
    def test_unexisting_part(self):

        conn = boto.connect_s3()
        conn.create_bucket(self.bucket_name)

        #actual computation
        part_id = 0
        document_uri = compute_zones(self.s3_uri,
                                     part_id)

        #check output
        expected_document_uri = "{}/zones.txt.{}.gz".format(self.s3_uri, part_id)
        self.assertEqual(expected_document_uri, document_uri)

        zone_stream = ZoneStreamDef.get_stream_from_s3(self.s3_uri,
                                                       tmp_dir=self.tmp_dir,
                                                       part_id=part_id)
        self.assertEqual([], list(zone_stream))