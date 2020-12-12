import json

class JsonHandler():
    def __init__(self, data):
        """ takes either a file path or json-type object"""
        if isinstance(data, str):
            with open(data) as f:
                self.data = json.loads(f.read())
        else:
            self.data = data
    
        self.fields = self.get_fields()
    
    def get_fields(self):
        pass
    
    def get_sample(self, sample_size=5, unique=True):
        pass
    
    def to_excel(self, file_name, divisions=None):
        pass
    
    def num_entries(self):
        pass
    
    def num_potential(self):
        pass
    
    def percent_na(self):
        pass
    
    def consistency_check(self):
        pass


file_path = '/home/adam/Documents/Portfolio/JSON_Explorer/example.json'

test = JsonHandler(file_path)
data = test.data
