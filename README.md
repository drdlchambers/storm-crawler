storm-crawler
=============

Distributed Storm Web Crawler


Performs a parallel crawl of the initial seed URLs listed in the UlrQueueSpout3.java file.  Counts of the unique 
words per page are extracted using a Python script and results are stored in a local PostgreSQL database (access 
credentials set in UrlWriter.java and WordWriter.java bolt files).

Default crawl depth is 10 (can be adjusted in Spout) and a 1 second domain-specific throttle is used (can be 
adjusted in ExtractorBolt.java file.  In addition, a running HashMap crawl history is used to prevent duplicate 
page crawls.
