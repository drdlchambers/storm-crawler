import storm

import re, string, urllib, urlparse, time

from bs4 import BeautifulSoup


class UrlScraper(object):
    
    def __init__(self):
        self.links = []
        self.current_page = ""
    
    
    def return_links(self, data, parent_url):
        self.current_page = parent_url
        self.extract_links(data)
        return self.links
    
    
    def extract_links(self, data):
        soup = BeautifulSoup(data)
        
        for link in soup.find_all('a'):
            
            if (link.get('href')):
                #Whitelist Acceptable URL Types (only crawling academic sites)
                m = re.findall(r'(\/$)|(\.php$)|(\.htm$)|(\.html$)|(\.asp$)|(\.jsp$)|(\.edu$)', link.get('href'))
                if m:
                    
                    if ( ((link.get('href'))[:5] == 'http:') or ((link.get('href'))[:6] == 'https:') ):
                        self.links.append(link.get('href').split('#')[0])   # remove hash tags from URL
                    
                    elif ((link.get('href'))[:1] == '#'):
                        """ do nothing """                                  # ignore links to the current page
                    
                    else:
                        base_link = link.get('href').split('#')[0]
                        base_link_parse = urlparse.urlparse(base_link)
                        
                        if (base_link_parse.scheme == ''):                  # only want fragments (eliminate FTP and other protocols)
                            fq_url = urlparse.urljoin(self.current_page,base_link)
                            self.links.append(fq_url)

    

class ScrapeUrlBolt(storm.BasicBolt):
    def process(self, tup):
        rawHtml = tup.values[0]
        current_page = tup.values[1]
        domain = tup.values[2]
        current_depth = tup.values[3]
        new_depth = int(current_depth) + 1

        ps = UrlScraper()
        extraced_links = ps.return_links(rawHtml,current_page)
        
        for link in extraced_links:
            temp_url = urlparse.urlparse(link)
            # This will FORCE crawler to stay on current domain
            if (domain in temp_url.netloc):
                storm.emit([link,current_page,domain,new_depth])
        

ScrapeUrlBolt().run()




