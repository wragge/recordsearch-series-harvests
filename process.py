import csv
from pymongo import MongoClient

try:
    from credentials import MONGOLAB_URL
except ImportError:
    MONGOLAB_URL = 'mongodb://localhost:27017/naturalisations'


def export_csv(series_id):
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    with open('data/{}.csv'.format(series_id), 'wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([
            'barcode',
            'series',
            'control_symbol',
            'title',
            'start_date',
            'end_date',
            'access_status',
            'location',
            'digitised_status',
            'digitised_pages'
        ])
        items = db.items.find({'series': series_id})
        # print len(list(items))
        for index, item in enumerate(items):
            csv_writer.writerow([
                index,
                item['identifier'],
                item['series'],
                item['control_symbol'],
                item['title'].replace('\n', ' ').replace('\r', '').replace('  ', ' '),
                item['contents_dates']['start_date'],
                item['contents_dates']['end_date'],
                item['access_status'],
                item['location'],
                item['digitised_status'],
                item['digitised_pages']
            ])
