import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/')) for x in ['src/excel/include', 'src/excel/numformat/include']
]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['src/excel/excel_extension.cpp']]
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'src/excel/numformat/nf_calendar.cpp',
        'src/excel/numformat/nf_localedata.cpp',
        'src/excel/numformat/nf_zformat.cpp',
        'src/excel/xlsx/read_xlsx.cpp',
        'src/excel/xlsx/write_xlsx.cpp',
        'src/excel/xlsx/zip_file.cpp',
    ]
]
