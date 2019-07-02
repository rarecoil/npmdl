# npmdl

## A multithreaded NPM registry downloader

This is a dumb multithreaded Python script I wrote to download the latest
version of every npm module that exists in the NPM registry. Uses one thread
to populate a Redis queue from skimdb and 10 to download modules.

I guess it's also an OK example of how to use a Redis worker queue in Python.

## License

MIT