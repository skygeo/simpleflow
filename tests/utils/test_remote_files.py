import unittest
import boto
import mock
import json
from moto import mock_s3
from cdf.exceptions import MalformedFileNameError
from cdf.utils.remote_files import (
    enumerate_partitions,
    get_part_id_from_filename,
    get_crawl_info,
    get_max_crawled_partid
)
from tests.mixins import TestBucketMixin


class TestEnumeratePartitions(unittest.TestCase, TestBucketMixin):

    @mock_s3
    def test_nominal_case(self):
        test_bucket = self._get_bucket()
        self._create_file(test_bucket, "urlids.txt.0.gz")
        self._create_file(test_bucket, "urlids.txt.4.gz")

        self.assertEquals([0, 4], enumerate_partitions("s3://test_bucket/", 100, 200))

    @mock_s3
    def test_misc_files(self):
        test_bucket = self._get_bucket()
        self._create_file(test_bucket, "urlids.txt.0.gz")
        self._create_file(test_bucket, "urlinfos.txt.4.gz")

        self.assertEquals([0], enumerate_partitions("s3://test_bucket/", 100, 200))

    @mock_s3
    def test_regex_special_character(self):
        test_bucket = self._get_bucket()
        #check that "." in regex is not interpret as wildcard
        self._create_file(test_bucket, "urlids_txt_0_gz")
        #check that only files ending with the patterns are considered
        self._create_file(test_bucket, "urlids_txt_0_gz.foo")

        self.assertEquals([], enumerate_partitions("s3://test_bucket/", 100, 200))

    @mock_s3
    def test_enumerate_partions_crawled_urls(self):
        test_bucket = self._get_bucket()
        self._create_file(test_bucket, "urlids.txt.0.gz")
        self._create_file(test_bucket, "urlids.txt.1.gz")
        self._create_file(test_bucket, "urlids.txt.2.gz")
        self._create_file(test_bucket, "files.json")
        with mock.patch('cdf.utils.remote_files.get_crawl_info') as get_crawl_info:
            get_crawl_info.return_value = {"max_uid_we_crawled": 1025}
            self.assertEquals([0, 1],
                              enumerate_partitions("s3://test_bucket/",
                                                   first_part_id_size=1024,
                                                   part_id_size=300000,
                                                   only_crawled_urls=True))


class TestGetPartIdFromFileName(unittest.TestCase):
    def test_nominal_case(self):
        self.assertEqual(0, get_part_id_from_filename("urlcontents.txt.0.gz"))
        self.assertEqual(10, get_part_id_from_filename("urlcontents.txt.10.gz"))
        self.assertEqual(0, get_part_id_from_filename("/tmp/urlcontents.txt.0.gz"))

    def test_malformed_filename(self):
        self.assertRaises(MalformedFileNameError,
                          get_part_id_from_filename,
                          "urlcontents.txt.gz")

        self.assertRaises(MalformedFileNameError,
                          get_part_id_from_filename,
                          "urlcontents.txt.-1.gz")


class TestGetCrawlInfo(unittest.TestCase, TestBucketMixin):

    @mock_s3
    def test_nominal_case(self):
        test_bucket = self._get_bucket()
        crawl_info = {"max_uid_we_crawled": 1000}
        self._create_json_file(test_bucket,
                               "files.json",
                               crawl_info)
        self.assertEquals(get_crawl_info("s3://test_bucket"),
                          crawl_info)


class TestGetMaxCrawledPartId(unittest.TestCase):

    def test_nominal_case(self):
        # Test 1 url
        crawl_info = {"max_uid_we_crawled": 1}
        self.assertEquals(
            get_max_crawled_partid(crawl_info, 100, 200),
            0
        )

        # Test 101 urls
        crawl_info["max_uid_we_crawled"] = 101
        self.assertEquals(
            get_max_crawled_partid(crawl_info, 100, 200),
            1
        )

        # Test 301 urls
        crawl_info["max_uid_we_crawled"] = 301
        self.assertEquals(
            get_max_crawled_partid(crawl_info, 100, 200),
            2
        )
