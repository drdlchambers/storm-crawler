import storm

import re, string, time

from nltk import FreqDist
from nltk.tokenize import word_tokenize


class PageScraper(object):
    
    def __init__(self):
        self.word_count = {}
            
    
    def scrape_page(self, data):
        self.word_frequency(data)
            
    
    def clean_text(self, raw_data):
        # remove HTML/CSS/Javascript, Punctuation, and Numbers
        
        raw_data = raw_data.lower()                                                 # make everything lowercase
        raw_data = re.sub("<head>.*?</head>", "", raw_data, flags=re.DOTALL)        # remove HTML Header
        raw_data = re.sub("<script.*?</script>", "", raw_data, flags=re.DOTALL)     # remove all script blocks
        raw_data = re.sub("<style.*?</style>", "", raw_data, flags=re.DOTALL)       # remove all style blocks
        raw_data = re.sub("<!--.*?-->", "", raw_data, flags=re.DOTALL)              # remove all comment blocks
        raw_data = re.sub("<.*?>", " ", raw_data, flags=re.DOTALL)                  # remove all HTML < > tags
        raw_data = re.sub('\r', "", raw_data, flags=re.DOTALL)                      # remove carriage returns
        raw_data = re.sub('\t', "", raw_data, flags=re.DOTALL)                      # remove tabs
        raw_data = re.sub('\|', " ", raw_data, flags=re.DOTALL)                     # remove pipes
        raw_data = re.sub('&nbsp;', " ", raw_data, flags=re.DOTALL)                 # remove HTML non-breaking spaces
        raw_data = re.sub('\'s', "", raw_data, flags=re.DOTALL)                     # remove apostraphe s
        raw_data = re.sub('&.*?;', " ", raw_data, flags=re.DOTALL)                  # remove HTML special characters
        raw_data = re.sub(r'(?<=\w{1})\.(?=\w{1})', "", raw_data, flags=re.DOTALL)  # remove dots from abbreviations (u.s.a. => usa)
        raw_data = re.sub('\s{2,}', " ", raw_data, flags=re.DOTALL)                 # remove extraneous white spaces
        raw_data = re.sub('[%s]' % re.escape(string.punctuation), ' ', raw_data)    # remove punctuation
        raw_data = re.sub('[0-9]{1,}', " ", raw_data, flags=re.DOTALL)              # remove numbers
        
        return raw_data
    
    
    
    def word_frequency(self, raw_data):
        word_string = self.clean_text(raw_data)
        self.word_count = self.get_word_freq(word_string)
    
    
    def get_word_freq(self, document):
        all_words = word_tokenize(document)
        all_words_freq = FreqDist(all_words)
        return all_words_freq
        
    
    def return_word_count(self):
        return self.word_count;
        



class ScrapeWordsBolt(storm.BasicBolt):
    def process(self, tup):
        rawHtml = tup.values[0]
        current_page = tup.values[1]
        domain = tup.values[2]

        ps = PageScraper()
        ps.scrape_page(rawHtml)
        page_word_count = ps.return_word_count()
        
        # Filter Out Any Words Containing Non-Letters
        for word in page_word_count:
            cf_word = word
            cf_word = re.sub('[%s]' % (string.letters), "", cf_word)
            if (len(cf_word) == 0):
                temp_count = page_word_count[word]
                storm.emit([word,int(temp_count),current_page,domain])


ScrapeWordsBolt().run()







