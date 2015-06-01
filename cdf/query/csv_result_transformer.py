from cdf.query.result_transformer import ResultTransformer


class FlatNamesNormalizer(ResultTransformer):
    """
    Normalizes flat names : [{'redirect': {'to': {'urls': 'test'}}}]
    returns [{'redirect.to.urls': 'test'}]
    """

    def __init__(self, data_format):
        self.data_format = data_format

    def _key_is_custom_field(self, key):
        try:
            if 'csv' in self.data_format[key]:
                return True
            return False
        except KeyError:
            return False

    def _flatten_column(self, columns):
        # In some cases, one dict entry can flatten to several dict entries e.g. {u'redirect': {u'to': {u'url': None},
        # u'from': {u'urls': [[u'http://fr.ulule.com/projects/?_escaped_fragment_=status/success/', 301]]}} flattens to
        # {'redirect.to.url': None, 'redirect.from.urls':
        # [[u'http://fr.ulule.com/projects/?_escaped_fragment_=status/success/', 301]]}
        # We also do not flatten further keys that already are names of custom fields to avoid
        # {'redirect.to.url.url': 'a', 'redirect.to.url.http_code': 'b'}
        new_columns = {}
        for (key, value) in columns.iteritems():
            key_is_custom_field = self._key_is_custom_field(key)

            if not key_is_custom_field and isinstance(value, dict):
                new_columns.update(self._flatten_column({key + '.' + new_key: value[new_key]
                                   for new_key in value.keys()}))
            else:
                new_columns.update({key: value})
        return new_columns

    def transform(self, results):
        for index, result in enumerate(results):
            flattened_columns = [self._flatten_column({key: value}) for (key, value) in result.iteritems()]
            results[index] = {k: v for d in flattened_columns for k, v in d.items()}
        return results


class MultipleFieldsNormalizer(ResultTransformer):
    """
    Flattens multiple fields : [{'redirect.to.urls': [1, 2, 3]}]
    returns [{'redirect.to.urls': '1'}, {'redirect.to.urls', 2}, {'redirect.to.urls': 3}]
    """

    def __init__(self, multiple_field):
        self.multiple_field = multiple_field

    def transform(self, results):
        index_offset = 0

        for index, tmp_result in enumerate(list(results)):
            if self.multiple_field in tmp_result and tmp_result[self.multiple_field] != []:
                # For each row in tmp_result,
                # we iterate over the row's multiple field's values and replace the row with flattened data
                # We must calculate the 'index offset' (number of rows added in the middle) to replace the correct data
                # (if we added rows, the indexes are not the same)
                # For 'redirect.to.urls' as multiple_field and tmp_result = {'redirect.to.urls': [1, 2],
                # 'something': 'a'}
                # values is [{'redirect.to.urls': 1, 'something': 'a', 'Counter': 1},
                # {'redirect.to.urls': 2, 'something': 'b', 'Counter': 2}
                values = [dict(tmp_result,
                               **{'Counter': multiple_index + 1,
                                  self.multiple_field: multiple_value})
                          for (multiple_index, multiple_value) in
                          enumerate(tmp_result.pop(self.multiple_field))]
                results[index + index_offset:index + index_offset + 1] = values
                index_offset += (len(values) - 1)
        return results


class VerboseNamesCustomFieldsNormalizer(ResultTransformer):
    """
    Applies custom strategy for custom fields.
    For non-custom fields, replaces keys by verbose names, using the 'verbose_name' definitions in the datamodel
    e.g. if we have "redirect.to.url": { "verbose_name": "Redirects to", [...] in the datamodel
    the result for {'redirect.to.url': 'something'} will be {'Redirects to': 'something'}
    """

    def __init__(self, data_format):
        self.data_format = data_format

    def _transform_column(self, key, value):
        try:
            data_format_entry = self.data_format[key]
            if 'csv' in self.data_format[key]:
                try:
                    values = self.data_format[key]['csv']['strategy'](value)
                    return {self.data_format[key]['csv']['fields'][new_key]: new_value for (new_key, new_value)
                            in values.iteritems()}
                except:
                    return {}
            return {data_format_entry['verbose_name']: value}
        except KeyError:
            # Some fields, like 'Counter', are not defined in the data format
            return {key: value}

    def transform(self, results):
        for index, result in enumerate(results):
            # If custom field : apply custom field strategy and return custom verbose keys
            # otherwise : look for the verbose key
            transformed_columns = [self._transform_column(key, value) for (key, value) in result.iteritems()]
            results[index] = {k: v for d in transformed_columns for k, v in d.items()}
        return results


def transform_csv_result(results, query):
    """
    Walk through every result and transform it for CSV output
    """
    if query.multiple_field:
        transformers = [FlatNamesNormalizer(query.data_format),
                        MultipleFieldsNormalizer(query.multiple_field),
                        VerboseNamesCustomFieldsNormalizer(query.data_format)]
    else:
        transformers = [FlatNamesNormalizer(query.data_format),
                        VerboseNamesCustomFieldsNormalizer(query.data_format)]

    for transformer in transformers:
        transformer.transform(results)
