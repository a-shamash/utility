import json

class JsonHandler():
    def __init__(self, data, delim='.'):
        """ takes either a file path or json-type object"""
        if isinstance(data, str):
            with open(data) as f:
                self.data = json.load(f)
        else:
            self.data = data
        
        self.delim = delim
        self.fields, self.arrays = self._get_fields(self.data)
    
    def get_fields(self, delim='.'):
        return sorted(map(lambda x: x.replace(self.delim, delim), self.fields))
          
    
    def _get_fields(self, data, fields=set(), multiples=set()):
        """Explores entire json and returns full list of fields"""
        
        if isinstance(data, list):  # case when json starts as list
            if isinstance(data[0], dict):
                for element in data:
                    f, m = self._get_fields(element, fields, multiples)
                    fields |= f
                    multiples |= m
            else:  # is this a proper JSON/option?
                return fields, multiples
        
        if isinstance(data, dict):
            for path, value in data.items():
                if isinstance(value, dict):
                    temp = {path + self.delim + k:v for k, v in value.items()}
                    f, _ = self._get_fields(temp)
                    fields |= f
                elif value and isinstance(value, list):
                    multiples.add(path)
                    if isinstance(value[0], dict):
                        for element in value:
                            temp = {path + self.delim + k:v for k, v in element.items()}
                            f, _ = self._get_fields(temp)
                            fields |= f    
                    else:
                        fields.add(path)
                else:
                    fields.add(path)
                    
        
        return fields, multiples
    
    
    def get_sample(self, sample_size=5, unique=True):
        pass
    
    def to_excel(self, file_name, divisions=None):
        pass
    
    def num_entries(self):
        pass
    
    def num_potential(self, fields):
        pass
    
    def percent_na(self):
        pass
    
    def consistency_check(self):
        pass


file_path = '/home/adam/Documents/Portfolio/JSON_Explorer/example.json'

test = JsonHandler(file_path)
data = test.data
