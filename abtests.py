#!/usr/bin/env python
# coding: utf-8

# In[9]:


import os.path
import pandas as pd
from os import mkdir, rename
from joblib import dump, load
from shutil import rmtree

class ABTest:
    
    def __init__(self, research_id : int, description, start_date, end_date, is_active=False):
        
        if not isinstance(research_id, int):
            raise ValueError(f'ID must be integer, not {type(research_id)}')
            
        self.ID = research_id
        self.DESCRIPTION = description
        self.START_DATE = start_date
        self.END_DATE = end_date
        self.IS_ACTIVE = is_active
        self.__LISTS_ARE_CREATED = False
        self.__A = None
        self.__B = None
        self.__CL_SOURCES = None
        
    def ABListsAreCreated(self):
        return self.__LISTS_ARE_CREATED
    
    def info(self):
        
        response_str = "Information about AB Test:\n"
        for attr_name, attr_val in self.__dict__.items():
            response_str += f"{str(attr_name)}:\t{str(attr_val)}\n"
        print(response_str)
        
    def addTestToStorage(self, storage):
        storage.addABTest(self)
        
    def getABLists(self):
        return self.__A, self.__B
    
    def saveABLists(self, pathA, pathB, pathSources):
        
        if self.ABListsAreCreated():
            self.__CL_SOURCES.to_csv(pathSources, index=False)
            self.__A.to_csv(pathA, index=False)
            self.__B.to_csv(pathB, index=False)
        else:
            raise BaseException("AB Lists are not created. Impossible to save.")
        
    def createABLists(self, lists, list_size, to_return = False):
        """Paramaters:
        \tlists : list of pandas.DataFrame objects containing columns 'CLIENT_ID', 'PROBA', 'SOURCE' and common columns structure
        \tlist_size : int, size of both lists A, B"""
        
        if not self.ABListsAreCreated():
            
            clients = pd.DataFrame(columns=lists[0].columns)
            clients_with_duplicates = clients.copy()
            
            num_of_lists = len(lists)
            
            lists_cpy = []
            
            for list_num in range(num_of_lists):
                lists_cpy.append(lists[list_num].copy())
                
            while clients['CLIENT_ID'].unique().shape[0] < list_size*2:
                clients = clients.append(lists_cpy[list_num].iloc[0:1])
                clients_with_duplicates = clients_with_duplicates.append(lists_cpy[list_num].iloc[0:1])
                
                clients = clients.drop_duplicates(['CLIENT_ID'])
                
                for list_num in range(num_of_lists):
                    lists_cpy[list_num] = lists_cpy[list_num].drop(index=0).reset_index(drop=True)
                    
            clients = clients.reset_index(drop=True)
            
            A = clients.sample(list_size).sort_values(by='PROBA', ascending=False)
            B = clients[~clients.index.isin(A.index)].sort_values(by='PROBA', ascending=False)
            
            self.__LISTS_ARE_CREATED = True
            self.__A = A
            self.__B = B
            self.__CL_SOURCES = clients_with_duplicates
            
            if to_return:
                return self.getABLists()
            
        else:
            raise BaseException("Lists have been already created. Use self.updateABLists() to create new lists or self.getABLists() to get lists")
            
        def updateABLists(self, lists, list_size, to_return = False):
            self.__LISTS_ARE_CREATED = False
            self.createABLists(lists, list_size)
            
            if to_return:
                return self.getABLists()
            
        def evaluateABTest(self, contracts, list_name, strategy='general'):
            """Paramaters:
            \tcontracts : pandas.DataFrame objects with contracts information containing columns 'CLIENT_ID', 'DATE_BEG' in format 'YYYY-MM-DD'
            \tlist_name : must be either 'A' or 'B'
            \tstrategy : must be either 'general' or 'ff'"""
            
            contracts_cpy = contracts.copy()
            contracts_cpy['CONTRACT_FLAG'] = 1
            
            if list_name == 'A':
                data = self.__A
            elif list_name == 'B':
                data = self.__B
            else:
                raise ValueError("list_name must be either 'A' or 'B'")
                
            data = data[['CLIENT_ID','SOURCE']]
            data = data.merge(contracts_cpy, on='CLIENT_ID', how='left')
            data['CONTRACT_FLAG'] = data['CONTRACT_FLAG'].fillna(0)
            data = data[['CLIENT_ID', 'SOURCE', 'COUNT_FLAG']].groupby(['CLIENT_ID','SOURCE']).sum().reset_index()
            data['CONTRACT_FLAG'] = (data['CONTRACT_FLAG'] > 0).astype('int')
            
            if strategy == 'by_source':
                evalueted = data.groupby('SOURCE')[['CONTRACT_FLAG']].mean() * 100
            elif strategy == 'general':
                evalueted = data['CONTRACT_FLAG'].mean() * 100
            else:
                raise ValueError("'strategy' parameter must by either 'by_source' or 'general'")
                
            return evalueted
        
class Storage:
    
    def __init__(self, file_path):
        
        content = pd.read_excel(file_path)
        
        if 'id' in content.columns:
            self.__FILE_NAME = os.path.split(file_path)[1]
            self.__FILE_PATH = os.path.abspath(file_path)
            self.__CONTENT = content
        else:
            raise ValueError("Storage file must contain at least 'id' column. All columns: 'id', 'description', 'start_date', 'end_date', 'is_active'")
        
    def getFileName(self):
        return self.__FILE_NAME
    
    def getFilePath(self):
        return self.__FILE_PATH
    
    def getContent(self, only_active = False):
        if only_active:
            return self.__CONTENT[self.__CONTENT['is_active'] == 1]
        else:
            return self.__CONTENT
        
    def getABTestByID(self, research_id):
        
        file_content = self.getContent()
        research_row = file_content[file_content['id'] == research_id]
        
        if research_row.shape[0] == 0:
            raise ValueError(f"No such AB Test with ID = {str(research_id)}")
            
        description = research_row['description'].values[0]
        start_date = research_row['start_date'].values[0]
        end_date = research_row['end_date'].values[0]
        is_active = research_row['is_active'].values[0]
        
        return ABTest(research_id, description, start_date, end_date, is_active)
    
    def addABTest(self, abtest):
        
        catalog_name = f'abtest_id_{abtest.ID}'
        
        if abtest.ID in self.__CONTENT['id'].values or os.path.exists(catalog_name):
            raise ValueError(f"AB Test with ID = {str(abtest.ID)} already exists in the Storage. Create a unique ID or use self.updateABTest()")
        else:
            mkdir(catalog_name)
            self.__CONTENT = self.__CONTENT.append(
                pd.DataFrame([{
                    'id' : abtest.ID,
                    'description' : abtest.DESCRIPTION,
                    'start_date' : abtest.START_DATE,
                    'end_date' : abtest.END_DATE,
                    'is_active' : abtest.IS_ACTIVE
                    
                }])
            ).reset_index(drop=True)
            
            dump(abtest, os.path.join(catalog_name, f'abtest_id_{abtest.ID}.bin'))
            
            if abtest.ABListsAreCreated():
                abtest.saveABLists(os.path.join(catalog_name, 'B.csv'), os.path.join(catalog_name, 'client_sources.csv'))
            
            self.save()
            
    def removeABTestByID(self, research_id):
        if research_id not in self.__CONTENT['id']:
            pass
        else:
            #rename(f'abtest_id_{research_id}', f'abtest_{research_id}_to_delete_{datetime.now().date()}-{datetime.now().hour}-{datetime.now().minute}-{datetime.now().second}')
            rmtree(f'abtest_id_{research_id}')

            self.__CONTENT = self.__CONTENT.drop(index=self.__CONTENT[self.__CONTENT['id'] == research_id].index)

            self.save()

    def changeABTestInfo(self, research_id, columns_name, new_value):
        if column_name == 'id':
            raise ValueError('Changing ID is forbidden. Use self.removeABTestByID() and self.addABTest()')
        else:
            self.__CONTENT.loc[self.__CONTENT['id'] == research_id, column_name] = new_value

            self.save()

    def updateABTest(self, abtest):

        catalog_name = f'abtest_id_{abtest.ID}'

        if os.path.exists(catalog_name):
            dump(abtest, os.path.join(catalog_namem, f'abtest_id_{abtest.ID}.bin'))
            if abtest.ABListsAreCreated():
                abtest.saveABLists(os.path.join(catalog_name, 'B.csv'), os.path.join(catalog_name, 'client_sources.csv'))
        else:
            raise ValueError(f"AB Test with ID = {abtest.ID} does not exist in the Storage. Use self.addABTest() first")

    def save(self):
        self.__CONTENT.to_excel(self.__FILE_PATH, index=False)

