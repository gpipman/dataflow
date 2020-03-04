import os
import tabulator
from tabulator import Stream
import datapackage
from datapackage import Package
from dataflows import dump_to_path
#exec(open("./gustest.py").read())
dp = datapackage
dp.infer('gustest.csv')
'''
{'profile': 'tabular-data-package', 'resources': [{'path': 'gustest.csv', 'profile': 'tabular-data-resource', 'name': 'gustest', 'format': 'csv', 'mediatype': 'text/csv', 'encoding': 'utf-8', 'schema': {'fields': [{'name': 'Name', 'type': 'string', 'format': 'default'}, {'name': 'Surname', 'type': 'string', 'format': 'default'}], 'missingValues': ['']}}]}
'''
package = Package()
package.infer('./*.csv')
'''
{'profile': 'tabular-data-package', 'resources': [{'path': 'gustest.csv', 'profile': 'tabular-data-resource', 'name': 'gustest', 'format': 'csv', 'mediatype': 'text/csv', 'encoding': 'utf-8', 'schema': {'fields': [{'name': 'Name', 'type': 'string', 'format': 'default'}, {'name': 'Surname', 'type': 'string', 'format': 'default'}], 'missingValues': ['']}}]}
'''
package.save('data_test/gustest.json')
#True
print(package.get_resource('gustest').read(keyed=True))
'''
[{'Name': 'Gustavo', 'Surname': 'Pipman'}, {'Name': 'Beatriz', 'Surname': 'Pipman'}, {'Name': 'Asaf', 'Surname': 'Pipman'}, {'Name': 'Itamar', 'Surname': 'Pipman'}, {'Name': 'Netanel', 'Surname': 'Pipman'}, {'Name': 'Gal', 'Surname': 'Pipman'}]
'''
'''
Tabulator also supports multiline headers for the xls and xlsx formats.

with Stream('data.xlsx', headers=[1, 3], fill_merged_cells=True) as stream:
  stream.headers # ['header from row 1-3']
  stream.read() # [['value1', 'value2', 'value3']]
'''

print('\nTabulator with OPEN\n')
stream = tabulator.Stream('gustest.ods')
try:
    stream.open()
except tabulator.TabulatorException as e:
    pass  # Handle exception
for row in stream.iter():
    print(row)  # [value1, value2, ...]

stream.reset()  # Rewind internal file pointer
rows = stream.read()
stream.close()

print('\nTabulator with ODS\n')
with Stream('gustest.ods', headers=1) as stream:
    stream.headers # [header1, header2, ..]
    for row in stream:
        print(row)  # [value1, value2, ..]
'''
['Name', 'Surname']
['Gustavo', 'Pipman']
['Beatriz', 'Pipman']
['Asaf', 'Pipman']
['Itamar', 'Pipman']
['Netanel', 'Pipman']
['Gal', 'Pipman']
'''
print('\nTabulator with XLSX\n')
with Stream('gustest.xlsx', headers=1,sheet='Birthdate') as stream:
    stream.headers # [header1, header2, ..]
    for row in stream:
        print(row)  # [value1, value2, ..]


print('\nTabulator with CSV\n')
with Stream('gustest.csv', headers=1) as stream:
    stream.headers # [header1, header2, ..]
    for row in stream:
        print(row)  # [value1, value2, ..]

