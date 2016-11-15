# RecordSearch Series Harvests

Code to harvest the metadata and digitised images of all items in a series from the National Archives of Australia.

Note this will only work with series that have less than 20,000 items because of RecordSearch limits, but it could be easily modified to harvest a subset (or indeed a series of subsets).

## Start harvesting

The metadata is saved into a MongoDB database. This can be local, or on a cloud service like [mLab](https://mlab.com). Just copy `credentials_blank.py` to `credentials.py` and add in your database's url.

You'll need to git clone my [recordsearch-tools](https://github.com/wragge/recordsearch_tools) repository into a directory called `rstools`. Then in Python you can just:

``` python
import harvest
# Initiate harvester with a series id
harvester = harvest.SeriesClient(series='A712')
# Harvest item metadata
harvester.do_harvest()
# Harvest ALL the digitised images in this series
harvest.harvest_images()
```

Note that harvest_images() is set up to create derivatives of every image. To disable this, just delete the contents of the `IMAGE_SIZES` list in `harvest.py`.

To save the metadata as a CSV file:

``` python
import process
process.export_csv('A712')
```

To save a summary of the harvested series to a CSV file:

``` python
import harvest
harvest.series_summary(['A712', 'A711'])
```

## Harvested series

* [Series summary](data/series_summary.csv)
* [A711](data/A711.csv)
* [A712](data/A712.csv)

