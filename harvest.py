from rstools.client import RSSearchClient, RSSeriesClient
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, BulkWriteError, CursorNotFound
import time
import csv
import os
import requests
from PIL import Image, ImageOps
from StringIO import StringIO
from requests import ConnectionError
from rstools.utilities import retry

from credentials import MONGO_URL

IMAGES_DIR = 'images'

# This is for thumbnails. Just delete the contents ane leave an empty list if you don't want any.
# IMAGE_SIZES = [(200, 200), (500, 500)]
IMAGE_SIZES = []

# Used for harvest_all_series() and series_summary
SERIES_LIST = []


class SeriesHarvester():
    def __init__(self, series, control=None):
        self.series = series
        self.control = control
        self.total_pages = None
        self.pages_complete = 0
        self.client = RSSearchClient()
        self.prepare_harvest()
        db = self.get_db()
        self.items = db.items

    def get_db(self):
        dbclient = MongoClient(MONGO_URL)
        db = dbclient.get_default_database()
        # items = db.items
        # items.remove()
        return db

    def get_total(self):
        return self.client.total_results

    def get_db_total(self):
        return self.items.find({'series': self.series}).count()

    def prepare_harvest(self):
        if self.control:
            self.client.search(series=self.series, control=self.control)
        else:
            self.client.search(series=self.series)
        total_results = self.client.total_results
        print '{} items'.format(total_results)
        self.total_pages = (int(total_results) / self.client.results_per_page) + 1
        print self.total_pages

    @retry(ConnectionError, tries=20, delay=10, backoff=1)
    def start_harvest(self, page=None):
        if not page:
            page = self.pages_complete + 1
        while self.pages_complete < self.total_pages:
            if self.control:
                response = self.client.search(series=self.series, page=page, control=self.control, sort='9')
            else:
                response = self.client.search(series=self.series, page=page, sort='9')
            try:
                self.items.insert_many(response['results'])
            # Probably a duplicate error
            except BulkWriteError as bwe:
                # Find where the first error happened
                position = next((index for (index, d) in enumerate(response['results']) if d['identifier'] == bwe.details['writeErrors'][0]['op']['identifier']), None)
                # Slice the results set to start from where the error happened
                results = response['results'][position:]
                # Process records individually and handle duplicate errors
                for result in results:
                    try:
                        self.items.insert_one(result)
                    except DuplicateKeyError:
                        print 'Duplicate of {}'.format(result['identifier'])
            self.pages_complete += 1
            page += 1
            print '{} pages complete'.format(self.pages_complete)
            time.sleep(1)

    @retry((ConnectionError, CursorNotFound), tries=20, delay=10, backoff=1)
    def harvest_images(self):
        db = self.get_db()
        items = db.items.find({'series': self.series, 'digitised_status': True}).batch_size(10)
        images = db.images
        headers = {'User-Agent': 'Mozilla/5.0'}
        for item in items:
            directory = os.path.join(IMAGES_DIR, '{}/{}-[{}]'.format(self.series.replace('/', '-'), item['control_symbol'].replace('/', '-'), item['identifier']))
            if not os.path.exists(directory):
                os.makedirs(directory)
            for page in range(1, item['digitised_pages'] + 1):
                filename = '{}/{}-p{}.jpg'.format(directory, item['identifier'], page)
                print '{}, p. {}'.format(item['identifier'], page)
                if not os.path.exists(filename):
                    img_url = 'http://recordsearch.naa.gov.au/NaaMedia/ShowImage.asp?B={}&S={}&T=P'.format(item['identifier'], page)
                    response = requests.get(img_url, headers=headers, stream=True, verify=False)
                    response.raise_for_status()
                    try:
                        image = Image.open(StringIO(response.content))
                    except IOError:
                        print 'Not an image'
                    else:
                        width, height = image.size
                        image.save(filename)
                        del response
                        image_meta = {
                            'item_id': item['_id'],
                            'identifier': item['identifier'],
                            'page': page,
                            'width': width,
                            'height': height
                        }
                        images.save(image_meta)
                        print 'Image saved'
                        if IMAGE_SIZES:
                            os.makedirs(os.path.join(directory, 'thumbs'))
                            for size in IMAGE_SIZES:
                                new_width, new_height = size
                                thumb_file = '{}/thumbs/{}-p{}-{}-sq.jpg'.format(directory, item['identifier'], page, new_width)
                                thumb_image = ImageOps.fit(image, size, Image.ANTIALIAS)
                                thumb_image.save(thumb_file)
                            thumb_file = '{}/thumbs/{}-p{}-200.jpg'.format(directory, item['identifier'], page)
                            thumb_image = image.copy()
                            thumb_image.thumbnail((200, 200))
                            thumb_image.save(thumb_file)
                            image.close()
                            thumb_image.close()
                    time.sleep(1)


def harvest_all_series():
    for series in SERIES_LIST:
        print 'Series {}'.format(series['series'])
        if series['range']:
            for symbol in range(series['range'][0], series['range'][1]):
                print 'Control symbol {}'.format(symbol)
                harvester = SeriesHarvester(series=series['series'], control='*{}/*'.format(symbol))
                harvester.start_harvest()
        else:
            harvester = SeriesHarvester(series=series['series'])
            harvester.start_harvest()


def get_db_items():
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    items = db.items
    # items.remove()
    return items


def delete_one_series(series):
    items = get_db_items()
    deleted = items.delete_many({'series': series})
    print '{} items deleted'.format(deleted.deleted_count)


def change_to_int():
    '''
    I think this has been fixed in recordsearch_tools, so shouldn't need this any more.
    '''
    items = get_db_items()
    for record in items.find({'digitised_pages': {'$ne': 0}}).batch_size(30):
        record['digitised_pages'] = int(record['digitised_pages'])
        items.save(record)


def series_summary(series_list=SERIES_LIST):
    '''
    Creates a CSV file with summary data about the supplied series.
    This is summarising harvested data -- so you have to harvest the series first!
    Expects a list of series IDs.
    '''
    items = get_db_items()
    with open('data/series_summary.csv', 'wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['series', 'total described', 'total digitised', 'percentage digitised', 'total pages digitised'])
        for series in series_list:
            total = items.count({'series': series})
            total_digitised = items.count({'series': series, 'digitised_status': True})
            pipe = [{"$match": {"series": series}}, {"$group": {"_id": "$series", "total": {"$sum": "$digitised_pages"}}}]
            total_pages = items.aggregate(pipeline=pipe).next()['total']
            print series
            print 'Total: {}'.format(total)
            print 'Total digitised: {} ({:.2f}%)'.format(total_digitised, (total_digitised / float(total) * 100))
            print 'Total digitised pages: {}'.format(total_pages)
            csv_writer.writerow([series, total, total_digitised, '{:.2f}%'.format(total_digitised / float(total) * 100), total_pages])


def check_for_changes(series):
    items = get_db_items()
    digitised = items.count({'series': series, 'digitised_status': True})
    described = items.count({'series': series})
    access_open = items.count({'series': series, 'access_status': 'Open'})
    access_owe = items.count({'series': series, 'access_status': 'OWE'})
    access_nye = items.count({'series': series, 'access_status': 'NYE'})
    access_closed = items.count({'series': series, 'access_status': 'Closed'})
    client = RSSeriesClient()
    details = client.get_summary(series)
    print '\nNumber described: '
    print 'Database: {}'.format(described)
    print 'RecordSearch: {}'.format(details['items_described']['described_number'])
    print '\nNumber digitised:'
    print 'Database: {}'.format(digitised)
    print 'RecordSearch: {}'.format(details['items_digitised'])
    print '\nNumber open:'
    print 'Database: {}'.format(access_open)
    print 'RecordSearch: {}'.format(details['access_status']['OPEN'])
    print '\nNumber OWE:'
    print 'Database: {}'.format(access_owe)
    print 'RecordSearch: {}'.format(details['access_status']['OWE'])
    print '\nNumber NYE:'
    print 'Database: {}'.format(access_nye)
    print 'RecordSearch: {}'.format(details['access_status']['NYE'])
    print '\nNumber Closed:'
    print 'Database: {}'.format(access_closed)
    print 'RecordSearch: {}'.format(details['access_status']['CLOSED'])
