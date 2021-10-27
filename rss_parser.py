"""
    Type: Module
    Developer: Vignesh
    mail_id: vignesh@averyxgroup.com
    Description: Scraping rss content from URL and contains support functions
"""

### scrapers
import feedparser
from bs4 import BeautifulSoup as bs
### utils
import datetime
from dateutil.parser import parse
from time import mktime
import regex as re
from werkzeug.urls import url_fix


class RSS_Parser:
    def __init__(self):
        self.feedparser = feedparser
        self._remove_tags = ['img']

    """
        Parsing XML document using feedparser
    """
    def parse(self, url, last_poll, last_update, pattern):
        try:
            url = url_fix(url)
            parsed_list = []
            latest_update = last_update
            feed = self.feedparser.parse(url)
            if feed['entries']:
                for entry in feed['entries']:
                    if 'published' in entry:
                        published = parse(entry['published'])
                    else:
                        published = parse(entry['pubDate'])

                    if published > latest_update:
                        latest_update = published

                    if published < last_update:
                        continue

                    title = entry['title']


                    ### key selection, refer rss_vs_atom.config in ./
                    if 'summary' in entry:
                        key = 'summary'
                    elif 'description' in entry:
                        key = 'description'
                    else:
                        key = 'content'
                    ###

                    if pattern[-1] == key or pattern == []:
                        summary = entry[key]
                        summary = bs(summary, 'html.parser')
                        summary = self.remove_tags(summary)
                        summary = self.remove_tags_regex(str(summary))
                        summary = self.clean_text(str(summary))

                    else:
                        summary = bs(entry[key], 'html.parser')
                        summary = self.remove_tags(summary)
                        for element in pattern[1:]:
                            summary = summary.find_all(element)[0]
                        summary = summary.text
                        summary = self.remove_tags_regex(summary)

                    record = {
                        "title": title,
                        "summary": summary,
                        "published": published,
                    }

                    parsed_list.append(record)

                return parsed_list, latest_update
            else:
                return ["Reject"], None

        except Exception as e:
            self.log(e)
            return ["Exception", e], None

    """
        Removes unwanted tags from the summary
    """
    def remove_tags(self, html):
        for tag in self._remove_tags:
            for element in html.select(tag):
                element.extract()
        return html

    """
        Removes unwanted tags from summary using regex
    """
    def remove_tags_regex(self, html):
        compiled = re.compile(r'<.*?>')
        html = compiled.sub('', html)
        return html

    """
        String cleaning operations
    """
    def clean_text(self, text):
        text = re.sub(r"^\W+|\W+$", '', text)
        return text
    """
        Exception Logger
    """
    def log(self, e):
        #print(e)
        pass
    
    """
        Validate RSS link
    """
    def validate_link(self, feed):
        pass
    
