# web-directory-scraper
sometimes you get those annoying sites where it's just an index of files and folders, hopefully you know what im talking about. no easy way to just download the whole thing. this script solves that


This project downloads from web pages that serve a file index. First you run a script to get all of the urls to the different files hosted on the site,
then you run another script to actually download all the files. They go straight into S3. Some user setup is required, mainly provisioning an EC2 and an s3 bucket.
