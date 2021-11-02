import json
from random import sample as sample_funct

import pandas as pd


class JsonHandler():
    def __init__(self, data, delim: str='.'):
        """ takes either a file path or dict object"""
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
        
        self.summary_df = pd.DataFrame(sorted(self.fields), columns=['Fields'])
        
        self.summary_df[['Sample', 'DType', 'Array_Points', 'Array_Count', 'NA_Perc', 'Count']] = \
                                    (self.summary_df.Fields
                                        .apply(self._get_results)
                                        .apply(pd.Series))
        
        self.summary_df = self.summary_df.set_index('Fields')
        
    
    def _get_fields(self, data, fields=set(), arrays=set()):
        """Explores entire json and returns full list of fields+arrays"""
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
        """
        gets summary results for single filed
        Assumes field is a valid field path
        
        Parameters
        ----------
        field: str
            field, found with self.delim notation to get summary results for
        
        Returns
        ----------
        current_data
            sample values
        dtype
            data type of field
        array_points
            list of 1-M points
        array_count
            number of 1-M points
        na_paths/total
            nan percent
        len(current_data)
            total number of non-null values for record        
        """
        path = field.split(self.delim)
        
        if isinstance(self.data, list):
            current_data = self.data
        else:
            current_data = [self.data]
        
        na_paths = 0
        array_points = []
        
        for i, step in enumerate(path):
            if isinstance(current_data[0], list):
                current_data = [i for d in current_data for i in d]
                array_points.append(path[i-1])
                
            current_data = [i.get(step, None) for i in current_data]
            na_paths += current_data.count(None)
            current_data = [i for i in current_data if i is not None]
        
        if isinstance(current_data[0], list):
                array_points.append(path[-1])
            
        total = na_paths + len(current_data)
        
        try:
            value = current_data[0]
            dtype = ''
            if isinstance(value, list):
                dtype = 'List of '
                value = value[0]
            
            dtype += type(value).__name__
        except IndexError:
            dtype = 'List of Unknown'
        
        return (current_data,
                dtype,
                array_points,
                len(array_points),
                na_paths/total,
                len(current_data),)
        
    
    def get_fields(self, delim='.'):
        return sorted(map(lambda x: x.replace(self.delim, delim), self.fields))
    
    
    def get_sample(self, field, sample_size=5, unique=True):
        if field not in self.fields:
            raise Exception('Field {} is not found in data'.format(field))
        
        sample = self.summary_df.loc[field, 'Sample']
        
        if unique:
            sample = set(sample)
        
        return sample_funct(sample, min(sample_size, len(sample)))
    
        
    def to_relational(self):
        """transforms json to relational tables list"""
        # IDEA allow arbitrary divisions
        divisions = self.arrays.copy()
        
        
        ################################################
        # get columns for each table
        ################################################
        divisions = sorted(divisions, 
                           key=lambda x: x.count(self.delim), 
                           reverse=True)
 
        col_lists = dict()
        main_fields = self.fields.copy()
        
        for branch in divisions:
            col_lists[branch] = [f for f in main_fields if f.startswith(branch)]
            main_fields = [f for f in main_fields if not f.startswith(branch)]

        tables = dict()
        
        ################################################
        # create base table
        ################################################
        data = self.data.copy()
        # create base table       
        if isinstance(data, dict):
            base = pd.DataFrame()
            # TODO case when starting data is list
            for key, value in data.items():
                if isinstance(value, dict):
                    temp = pd.DataFrame.from_dict({key: value}, 'index')
                    temp.columns = [key+self.delim+c for c in temp.columns]
                    base = pd.concat([base, temp], axis=1)

                else:
                    base[key] = value
            
            base = base.reset_index(drop=True)
            base = self._expand(base)
        
        tables['main'] = base
        ################################################
        # create relational tables
        ################################################
        remaining_div = divisions
        while remaining_div:
            for div in divisions:
                tables_to_check = list(tables.keys())
                for check_key in tables_to_check:
                    if div in tables[check_key].columns:
                        current_table = tables[check_key]
                        current = current_table.loc[~pd.isnull(current_table[div]), [div]]
                        current = current.reset_index()
                        current = current.rename(columns={'index': f'{check_key}_ID'})
                        current = current.explode(div)
                        
                        current = self._expand(current, start_col=div)
                        
                        tables[div] = current
                        remaining_div.remove(div)
                        
                        tables[check_key] = current_table.drop(div, axis=1)
        
        return tables


    def _expand(self, table, start_col=''):
        table = table.reset_index(drop=True)
        while True:
            dict_fields = [c for c in table.columns 
                 if [d for d in self.fields if d.startswith(c) and d!=c]
                     and (c not in self.arrays or c == start_col)]
            
            
            if not dict_fields: break
            
            for key in dict_fields:
                
                temp = table[key].values.tolist()
                
                temp = pd.DataFrame(temp)
                
                temp.columns = [key+self.delim+c for c in temp.columns]
                
                
                table = pd.concat([table.drop(key, axis=1), temp], axis=1)
                
                
        return table
    
