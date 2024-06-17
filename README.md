# scrapers
Contains codes from different web scraping processes;

# fx_rate:
Scraper of public information on the exchange rates of several countries, each country has a different scraper.
The information is taken to redshift and an update and insertion process is carried out.

# external_prices:
In this case, the data is uploaded in S3 and then are copied with COPY method to insert the data in redshift.
