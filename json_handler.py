import json
import pandas as pd
from random import sample as sample_funct

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
        
        #########################################################
        # Fields: fields found in JSON
        #
        # Sample: sample of potential Values
        # NA_Perc: percent of potential paths without data
        # Count: Number of paths with data
        #########################################################
        
        self.df = pd.DataFrame(sorted(self.fields), columns=['Fields'])
        
        self.df[['Sample', 'NA_Perc', 'Count']] = (self.df.Fields
                                                    .apply(self._get_results)
                                                    .apply(pd.Series))
        
        self.df = self.df.set_index('Fields')
        
    
    def _get_fields(self, data, fields=set(), arrays=set()):
        """Explores entire json and returns full list of fields"""
        if isinstance(data, list):  # case when json starts as list
            if isinstance(data[0], dict):
                for element in data:
                    f, m = self._get_fields(element, fields, arrays)
                    fields |= f
                    arrays |= m
            else:  # is this a proper JSON/option?
                return fields, arrays
        
        if isinstance(data, dict):
            for path, value in data.items():
                if isinstance(value, dict):
                    temp = {path + self.delim + k:v for k, v in value.items()}
                    f, _ = self._get_fields(temp)
                    fields |= f
                elif value and isinstance(value, list):
                    arrays.add(path)
                    if isinstance(value[0], dict):
                        for element in value:
                            temp = {path + self.delim + k:v for k, v in element.items()}
                            f, _ = self._get_fields(temp)
                            fields |= f    
                    else:
                        fields.add(path)
                else:
                    fields.add(path)
                    
        
        return fields, arrays
    
    def _get_results(self, field):
        """Assumes field is a valid field path"""
        path = field.split(self.delim)
        
        if isinstance(self.data, list):
            current_data = self.data
        else:
            current_data = [self.data]
        
        na_paths = 0
        
        for step in path:
            if isinstance(current_data[0], list):
                current_data = [i for d in current_data for i in d]
   
            current_data = [i.get(step, None) for i in current_data]
            na_paths += current_data.count(None)
            current_data = [i for i in current_data if i is not None]
            
        total = na_paths + len(current_data)
        return current_data, na_paths/total, len(current_data)
        
    
    def get_fields(self, delim='.'):
        return sorted(map(lambda x: x.replace(self.delim, delim), self.fields))
    
    
    def get_sample(self, field, sample_size=5, unique=True):
        if field not in self.fields:
            raise Exception('Field {} is not found in data'.format(field))
        
        sample = self.df.loc[field, 'Sample']
        print(sample)
        if unique:
            sample = set(sample)
        
        return sample_funct(sample, min(sample_size, len(sample)))
    
        
    def to_excel(self, file_name, divisions=None):
        if divisions is None:
            divisions = self.arrays
    
    def num_entries(self):
        pass
    
    def num_potential(self, fields):
        pass
    
    def percent_na(self):
        pass
    
    def consistency_check(self):
        pass
