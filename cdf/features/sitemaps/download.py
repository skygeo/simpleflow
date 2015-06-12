import urlparse
import os.path
import time
from urllib3.exceptions import HTTPError

from cdf.log import logger

from cdf.features.sitemaps.exceptions import (UnhandledFileType,
                                             ParsingError,
                                             DownloadError)
from cdf.features.sitemaps.utils import download_url
from cdf.features.sitemaps.constant import DOWNLOAD_DELAY
from cdf.features.sitemaps.metadata import (SitemapMetadata,
                                           SitemapIndexMetadata,
                                           Error,
                                           Metadata)
from cdf.features.sitemaps.document import (SiteMapType,
                                           is_xml_sitemap,
                                           is_sitemap_index,
                                           is_rss_sitemap,
                                           is_text_sitemap,
                                           instanciate_sitemap_document)


def download_sitemaps(input_url, output_directory, user_agent, metadata):
    """Download all sitemap files related to an input url in a directory.
    If the input url is a sitemap, the file will simply be downloaded,
    if it is a sitemap index, it will download the listed sitemaps.
    The function returns a dict original url -> path.
    If a file could not be downloaded, the path is None.
    :param input_url: the url to the sitemap or sitemap index file
    :type input_url: str
    :param output_directory: the path to the directory where to save the files
    :type output_directory: str
    :param user_agent: the user agent to use for the query.
    :type user_agent: str
    :param metadata: an object that stores information about what has been
                     downloaded so far. It will be updated by the function.
    :type metadata: Metadata
    """
    if metadata.is_success_sitemap(input_url):
        #do not reprocess it
        return

    #download input url
    output_file_path = get_output_file_path(input_url, output_directory)
    try:
        download_url(input_url, output_file_path, user_agent)
    except (DownloadError, HTTPError) as e:
        logger.info("Download error: %s", str(e))
        metadata.add_error(
            Error(input_url, SiteMapType.UNKNOWN, e.__class__.__name__, str(e))
        )
        return

    try:
        sitemap_document = instanciate_sitemap_document(output_file_path, input_url)
    except UnhandledFileType as e:
        metadata.add_error(
            Error(input_url, SiteMapType.UNKNOWN, e.__class__.__name__, str(e))
        )
        return

    sitemap_type = sitemap_document.get_sitemap_type()
    #if it is a sitemap
    if is_xml_sitemap(sitemap_type) or is_rss_sitemap(sitemap_type) or is_text_sitemap(sitemap_type):
        metadata.add_success_sitemap(
            SitemapMetadata(input_url, sitemap_type, output_file_path)
        )
    #if it is a sitemap index
    elif is_sitemap_index(sitemap_type):
        #download referenced sitemaps
        download_sitemaps_from_sitemap_index(sitemap_document,
                                             output_directory,
                                             user_agent,
                                             metadata)
        #remove sitemap index file
        os.remove(output_file_path)
    else:
        error_message = "'{}' is not a valid file".format(input_url)
        metadata.add_error(
            Error(input_url, sitemap_type, "UnhandledFileType", error_message)
        )


def download_sitemaps_from_sitemap_index(sitemap_index_document,
                                         output_directory,
                                         user_agent,
                                         metadata):
    """Download sitemap files from a sitemap index.
    :param sitemap_index_document: the input sitemap index
    :type sitemap_index_document: SitemapIndexXmlDocument
    :param output_directory: the path to the directory where to save the files
    :type output_directory: str
    :param user_agent: the user agent to use for the query.
    :type user_agent: str
    :param metadata: an object that stores metadata about the download process
                     it will be modified by the function
    :type metadata: Metadata
    """
    url_generator = sitemap_index_document.get_urls()
    while True:
        try:
            url = url_generator.next()
        except ParsingError as e:
            #we can not recover parsing errors
            #so we update the download status
            update_download_status_on_parsing_error(metadata, sitemap_index_document, e)
            #and return it based on a partially processed sitemap index.
            return
        except StopIteration:
            break
        if metadata.is_success_sitemap(url):
                d_sitemaps = {
                    sitemap.url: sitemap for sitemap in metadata.sitemaps
                }
                sitemap_indexes = d_sitemaps[url].sitemap_indexes
                if not sitemap_index_document.url in sitemap_indexes:
                    #update its sitemap indexes
                    sitemap_indexes.append(sitemap_index_document.url)
                #do not reprocess the file
                continue

        file_path = get_output_file_path(url, output_directory)
        time.sleep(DOWNLOAD_DELAY)
        try:
            download_url(url, file_path, user_agent)
            sitemap_document = instanciate_sitemap_document(file_path, url)
        except (DownloadError, UnhandledFileType, HTTPError) as e:
            logger.info("Skipping {}: {}".format(url, str(e)))
            if os.path.isfile(file_path):
                os.remove(file_path)
            metadata.add_error(
                Error(url, SiteMapType.UNKNOWN, e.__class__.__name__, str(e))
            )
            continue

        sitemap_type = sitemap_document.get_sitemap_type()
        #  check if it is actually a sitemap
        if is_xml_sitemap(sitemap_type) or is_rss_sitemap(sitemap_type) or is_text_sitemap(sitemap_type):
            metadata.add_success_sitemap(
                SitemapMetadata(url, sitemap_type, file_path, [sitemap_index_document.url])
            )
        elif is_sitemap_index(sitemap_type):
            error_message = "'{}' is a sitemap index. It cannot be referenced in a sitemap index.".format(url)
            logger.info(error_message)
            metadata.add_error(
                Error(url, sitemap_type, "NotASitemapFile", error_message)
            )
            os.remove(file_path)
        else:
            #  if not, remove file
            error_message = "'{}' is not a sitemap file.".format(url)
            logger.info(error_message)
            metadata.add_error(
                Error(url, sitemap_type, "UnhandledFileType", error_message)
            )
            os.remove(file_path)

    metadata.add_success_sitemap_index(SitemapIndexMetadata(sitemap_index_document.url,
                                                            sitemap_index_document.valid_urls,
                                                            sitemap_index_document.invalid_urls))
    return


def update_download_status_on_parsing_error(download_status,
                                            sitemap_index_document,
                                            parsing_error):
    """Update the download status when a parsing error has been raised
    by a sitemap index document.
    If at least one url was found in the sitemap index, we consider it as a
    valid sitemap index document, otherwise we consider it as an error.
    :param download_status: the download status to update
    :type download_status: Metadata
    :param sitemap_index_document: the sitemap index that raised the parsing error
    :type sitemap_index_document: SitemapIndexXmlDocument
    :param parsing_error: the parsing error exception raised by sitemap_index_document
    :type parsing_error: ParsingError
    """
    if sitemap_index_document.total_urls > 0:
        #if we were able to process at least one url
        #report the sitemap index as success
        sitemap_index_metadata = SitemapIndexMetadata(
            sitemap_index_document.url,
            sitemap_index_document.valid_urls,
            sitemap_index_document.invalid_urls,
            parsing_error.__class__.__name__,
            str(parsing_error))
        download_status.add_success_sitemap_index(sitemap_index_metadata)
    else:
        #otherwise report it as error
        download_status.add_error(
            Error(sitemap_index_document.url,
                  SiteMapType.SITEMAP_INDEX,
                  parsing_error.__class__.__name__,
                  str(parsing_error))
        )


def get_output_file_path(url, output_directory):
    """Return the path where to save the content of an url.
    By default the function simply concatenates the output directory
    with the url basename.
    If the resulting path already exists, it appends a suffix "_2", "_3",
    until the resulting path does not exist.
    :param url: the input url
    :type url: str
    :param output_directory: the path to the directory
                             where to save the url content
    :type output_directory: str
    :returns: str
    """
    parsed_url = urlparse.urlparse(url)
    result = os.path.join(output_directory, os.path.basename(parsed_url.path))
    if not os.path.exists(result):
        return result
    #handle name collisions by appending a '_2','_3', etc. suffix
    index = 2
    while True:
        candidate_basename = "{}_{}".format(os.path.basename(parsed_url.path),
                                            index)
        candidate = os.path.join(output_directory, candidate_basename)
        if not os.path.exists(candidate):
            return candidate
        index += 1
