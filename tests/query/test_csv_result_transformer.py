import unittest
import mock

from cdf.core.features import Feature
from cdf.core.metadata.dataformat import assemble_data_format
from cdf.core.streams.base import StreamDefBase
from cdf.query.csv_result_transformer import (FlatNamesNormalizer, MultipleFieldsNormalizer,
                                              VerboseNamesCustomFieldsNormalizer)


class NormalizerTestCase(unittest.TestCase):
    def setUp(self):
        self.feature1 = Feature('feature1', 'feature1', None, None)
        # mock stream_def in feature
        self.feature1.get_streams_def = mock.Mock(return_value=[CustomStreamDef])
        with mock.patch('cdf.core.features.Feature.get_features') as mock_get_features:
            # mock features
            mock_get_features.return_value = [self.feature1]
            self.data_format = assemble_data_format()


class CustomStreamDef(StreamDefBase):
    URL_DOCUMENT_MAPPING = {
        "something_else": {
            "verbose_name": "Something Else",
            "type": "string"
        },
        "custom.field.url": {
            "verbose_name": "Custom Field",
            "type": "string",
            "csv": {
                "fields": "Custom Field Name",
                "strategy": lambda x: x[0]
            }
        },
        "multiple.field": {
            "verbose_name": "Multiple Field",
            "type": "string",
            "csv": {
                "fields": {
                    "http_code": "Multiple Field HTTP Code",
                    "url": "Multiple Field URL"
                },
                "strategy": lambda multiple_field: {'http_code': multiple_field[0],
                                                    'url': multiple_field[1]}
            }
        },
        "multiple_custom.field": {
            "verbose_name": "Multiple Field 2",
            "type": "string",
            "csv": {
                "fields": "Multiple Field Name 2",
                "strategy": lambda x: x[0]
            }
        },
    }


class FlatNamesNormalizerTestCase(NormalizerTestCase):

    def test_simple_flat_names(self):
        strat = FlatNamesNormalizer(self.data_format)
        es_result = [{u'redirect': {u'to': {u'url': {u'url': 'url4', u'crawled': False}}}}]

        expected_transform = [{u'redirect.to.url.url': 'url4', u'redirect.to.url.crawled': False}]

        self.assertEqual(strat.transform(es_result),
                         expected_transform)

    def test_list_flat_names(self):
        strat = FlatNamesNormalizer(self.data_format)
        es_result = [{u'redirect': {u'to': {u'url': [4, 300]}}}]

        expected_transform = [{u'redirect.to.url': [4, 300]}]

        self.assertEqual(strat.transform(es_result),
                         expected_transform)

    def test_multiple_field_flat_names(self):
        strat = FlatNamesNormalizer(self.data_format)
        es_result = [{u'redirect': {u'to': {u'urls': [[4, 300], [3, 300]]}}}]

        expected_transform = [{u'redirect.to.urls': [[4, 300], [3, 300]]}]

        self.assertEqual(strat.transform(es_result),
                         expected_transform)

    def test_custom(self):
        # We stop flattening when we encounter a custom field
        strat = FlatNamesNormalizer(self.data_format)
        es_result = [{u'custom': {u'field': {u'url': {u'url': 'url4', u'crawled': False}}}}]

        expected_transform = [{u'custom.field.url': {'url': 'url4', 'crawled': False}}]

        self.assertEqual(strat.transform(es_result),
                         expected_transform)


class MultipleFieldsNormalizerTestCase(unittest.TestCase):

    def test_no_values(self):
        strat = MultipleFieldsNormalizer('multiple.field')
        es_result = [{u'multiple.field': [], 'something_else': 'a'}]
        expected_transform = [{u'multiple.field': [], 'something_else': 'a'}]

        self.assertEqual(strat.transform(es_result),
                         expected_transform)

    def test_values(self):
        strat = MultipleFieldsNormalizer('multiple.field')
        es_result = [{u'multiple.field': [[4, 300], [3, 300]], 'something_else': 'a'}]
        expected_transform = [{u'Counter': 1, u'multiple.field': [4, 300], 'something_else': 'a'},
                              {u'Counter': 2, u'multiple.field': [3, 300], 'something_else': 'a'}]

        self.assertEqual(strat.transform(es_result),
                         expected_transform)


class VerboseNamesNormalizerTestCase(NormalizerTestCase):

    def test_custom_fields_verbose_names(self):
        strat = VerboseNamesCustomFieldsNormalizer(self.data_format)
        es_result = [{u'Counter': 1, u'multiple.field': [4, 400], 'something_else': 'a'},
                     {u'Counter': 2, u'multiple.field': [3, 300], 'something_else': 'a'}]
        expected_transform = [{u'Counter': 1, u'Multiple Field HTTP Code': 4, u'Multiple Field URL': 400,
                               u'Something Else': 'a'},
                              {u'Counter': 2, u'Multiple Field HTTP Code': 3, u'Multiple Field URL': 300,
                               u'Something Else': 'a'}]

        self.assertEqual(strat.transform(es_result),
                         expected_transform)


class AllNormalizersTestCase(NormalizerTestCase):

    def test_all_normalizers(self):
        # Apply all normalizers and check the result
        es_result = [{u'multiple': {u'field': [[4, 400], [3, 300]]}, 'something_else': 'a'}]
        expected_transform = [{u'Counter': 1, u'Multiple Field HTTP Code': 4, u'Multiple Field URL': 400,
                               u'Something Else': 'a'},
                              {u'Counter': 2, u'Multiple Field HTTP Code': 3, u'Multiple Field URL': 300,
                               u'Something Else': 'a'}]

        strat = FlatNamesNormalizer(self.data_format)
        strat2 = MultipleFieldsNormalizer('multiple.field')
        strat3 = VerboseNamesCustomFieldsNormalizer(self.data_format)

        strat.transform(es_result)
        strat2.transform(es_result)
        strat3.transform(es_result)
        self.assertEqual(es_result,
                         expected_transform)
