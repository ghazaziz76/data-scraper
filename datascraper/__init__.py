class WebExtractor:
    def extract_from_html(self, html, selectors):
        pass
        
    def extract_from_url(self, url, selectors):
        pass
        
    def extract_multiple(self, html, container, fields):
        pass

class APIExtractor:
    def extract(self, data, path):
        pass
        
    def extract_with_path(self, data, path):
        pass
        
    def extract_from_api(self, url, json_path):
        pass
