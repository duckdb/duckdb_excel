cmake_minimum_required(VERSION 2.8.12...3.29)
set(TARGET_NAME excel)
set(EXTENSION_NAME ${TARGET_NAME}_extension)

project(ExcelExtension)

include_directories(extension/excel/numformat/include)
include_directories(extension/excel/include)
add_subdirectory(extension/excel/numformat)

build_static_extension(excel extension/excel/excel_extension.cpp ${NUMFORMAT_OBJECT_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(excel ${PARAMETERS} extension/excel/excel_extension.cpp
                         ${NUMFORMAT_OBJECT_FILES})

install(
  TARGETS excel_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
