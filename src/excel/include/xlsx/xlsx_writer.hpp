#pragma once

#include "xlsx/zip_file.hpp"
#include "xlsx/xml_util.hpp"

namespace duckdb {

static constexpr auto CONTENT_TYPES_XML = R"(<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="xml" ContentType="application/xml"/><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="jpeg" ContentType="image/jpg"/><Default Extension="png" ContentType="image/png"/><Default Extension="bmp" ContentType="image/bmp"/><Default Extension="gif" ContentType="image/gif"/><Default Extension="tif" ContentType="image/tif"/><Default Extension="pdf" ContentType="application/pdf"/><Default Extension="mov" ContentType="application/movie"/><Default Extension="vml" ContentType="application/vnd.openxmlformats-officedocument.vmlDrawing"/><Default Extension="xlsx" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/xl/theme/theme1.xml" ContentType="application/vnd.openxmlformats-officedocument.theme+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>)";
static constexpr auto WORKBOOK_XML_START = 	R"(<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:mx="http://schemas.microsoft.com/office/mac/excel/2008/main" xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006" xmlns:mv="urn:schemas-microsoft-com:mac:vml" xmlns:x14="http://schemas.microsoft.com/office/spreadsheetml/2009/9/main" xmlns:x15="http://schemas.microsoft.com/office/spreadsheetml/2010/11/main" xmlns:x14ac="http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac" xmlns:xm="http://schemas.microsoft.com/office/excel/2006/main"><workbookPr/><sheets>)";
static constexpr auto WORKBOOK_XML_END = R"(</sheets><definedNames/><calcPr/></workbook>)";

static constexpr auto WORKSHEET_XML_START = R"(<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
           xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"
           xmlns:mx="http://schemas.microsoft.com/office/mac/excel/2008/main"
           xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
           xmlns:mv="urn:schemas-microsoft-com:mac:vml"
           xmlns:x14="http://schemas.microsoft.com/office/spreadsheetml/2009/9/main"
           xmlns:x15="http://schemas.microsoft.com/office/spreadsheetml/2010/11/main"
           xmlns:x14ac="http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac"
           xmlns:xm="http://schemas.microsoft.com/office/excel/2006/main">
<sheetData>
)";

static constexpr auto WORKSHEET_XML_END = R"(</sheetData></worksheet>)";

static constexpr auto WORKBOOK_REL_XML = R"(<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
		<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
		<Relationship Id="rId4" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
	</Relationships>
	)";


/*

<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme" Target="theme/theme1.xml"/>
		<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
		<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
 **/

class XLXSWriter {
public:
	void BeginSheet(const string &sheet_name, const vector<string> &sql_column_names, const vector<LogicalType> &sql_column_types);
	void EndSheet();

	explicit XLXSWriter(ClientContext &context, const string &file_name, idx_t sheet_row_limit_p) :
		stream(context, file_name), sheet_row_limit(sheet_row_limit_p) { }

	void WriteNumberCell(const string_t &value);
	void WriteStyledNumberCell(const string_t &value, idx_t style_idx);
	void WriteInlineStringCell(const string_t &value);
	void WriteEmptyCell();
	void BeginRow();
	void EndRow();

	void Finish();
private:
	idx_t WriteEscapedXML(const char *str);
	idx_t WriteEscapedXML(const string &str);
	idx_t WriteEscapedXML(const char *buffer, idx_t write_size);

	class XLSXSheet {
	public:
		string sheet_name;
		vector<string> sheet_column_names; // A1... Z1, AA1... ZZ1, etc.
		vector<string> sheet_column_types; // e.g. "str", "n", etc.
		vector<string> sql_column_names;
		vector<LogicalType> sql_column_types;
	};

	ZipFileWriter stream;
	idx_t sheet_row_limit;

	// Current sheet data;
	string row_str = "1";
	idx_t row_idx = 0;
	idx_t col_idx = 0;
	bool has_active_sheet = false;

	XLSXSheet active_sheet;
	vector<XLSXSheet> written_sheets;

	vector<char> escaped_buffer;
};

inline void XLXSWriter::BeginSheet(const string &sheet_name, const vector<string> &sql_column_names, const vector<LogicalType> &sql_column_types) {

	if(written_sheets.empty()) {
		// We need to create the directory for sheets
		stream.AddDirectory("xl/");
		stream.AddDirectory("xl/worksheets/");
	}

	D_ASSERT(!has_active_sheet);
	has_active_sheet = true;
	active_sheet.sheet_name = sheet_name;
	active_sheet.sql_column_names = sql_column_names;
	active_sheet.sql_column_types = sql_column_types;

	D_ASSERT(sql_column_names.size() == sql_column_types.size());
	const auto column_count = sql_column_names.size();

	// Generate sheet column names
	active_sheet.sheet_column_names.resize(column_count);
	for(idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		// Convert number to excel column name, e.g. 0 -> A, 1 -> B, 25 -> Z, 26 -> AA, etc.
		string col_name;
		idx_t col_num = col_idx + 1;
		while(col_num > 0) {
			col_name = static_cast<char>('A' + (col_num - 1) % 26) + col_name;
			col_num = (col_num - 1) / 26;
		}
		active_sheet.sheet_column_names[col_idx] = col_name;
	}

	// Generate sheet column types
	active_sheet.sheet_column_types.resize(column_count);
	for(idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		// Convert the logical type to the excel cell type identifier
		const auto &type = sql_column_types[col_idx];
		if(type.IsNumeric()) {
			active_sheet.sheet_column_types[col_idx] = "n";
		} else {
			active_sheet.sheet_column_types[col_idx] = "inlineStr";
		}
	}

	stream.BeginFile("xl/worksheets/sheet1.xml");
	stream.Write(WORKSHEET_XML_START);
}

inline void XLXSWriter::EndSheet() {
	D_ASSERT(has_active_sheet);
	has_active_sheet = false;
	stream.Write(WORKSHEET_XML_END);
	stream.EndFile();

	// Save the sheet
	written_sheets.push_back(std::move(active_sheet));

	row_str = "1";
	row_idx = 0;
	col_idx = 0;
}

inline void XLXSWriter::WriteNumberCell(const string_t &value) {
	stream.Write("<c r=\"" + active_sheet.sheet_column_names[col_idx] + row_str + "\" t=\"n\"><v>");
	stream.Write(value.GetData(), value.GetSize());
	stream.Write("</v></c>");

	col_idx++;
}

inline void XLXSWriter::WriteStyledNumberCell(const string_t &value, const idx_t style_idx) {
	stream.Write("<c r=\"" + active_sheet.sheet_column_names[col_idx] + row_str + "\" s=\"" + std::to_string(style_idx) + "\" t=\"n\"><v>");
	stream.Write(value.GetData(), value.GetSize());
	stream.Write("</v></c>");

	col_idx++;
}

inline void XLXSWriter::WriteInlineStringCell(const string_t &value) {
	stream.Write("<c r=\"" + active_sheet.sheet_column_names[col_idx] + row_str + "\" t=\"inlineStr\"><is><t>");
	// We need to escape this string in case it contains XML special characters
	WriteEscapedXML(value.GetData(), value.GetSize());
	stream.Write("</t></is></c>");

	col_idx++;
}

inline void XLXSWriter::WriteEmptyCell() {
	col_idx++;
}

inline void XLXSWriter::BeginRow() {
	stream.Write("<row r=\"" + row_str + "\">");
}

inline void XLXSWriter::EndRow() {
	stream.Write("</row>");
	col_idx = 0;

	row_idx++;
	row_str = std::to_string(row_idx + 1);

	if(row_idx > sheet_row_limit) {
		throw InvalidInputException("XLSX: Sheet row limit of '%s' rows exceeded!", sheet_row_limit);
	}
}

inline void XLXSWriter::Finish() {

	// Write other mandatory files
	stream.AddDirectory("_rels/");
	stream.AddDirectory("xl/_rels/");

	stream.BeginFile("[Content_Types].xml");
	stream.Write(CONTENT_TYPES_XML);
	stream.EndFile();

	stream.BeginFile("xl/workbook.xml");
	stream.Write(WORKBOOK_XML_START);
	stream.Write(R"(<sheet state="visible" name=")");
	WriteEscapedXML("Sheet 1"); // TODO: Escape the sheet name
	stream.Write(R"(" sheetId="1" r:id="rId4"/>)");
	stream.Write(WORKBOOK_XML_END);
	stream.EndFile();

	stream.BeginFile("xl/_rels/workbook.xml.rels");
	stream.Write(WORKBOOK_REL_XML);
	stream.EndFile();

	// Done!
	stream.Finalize();
}

inline idx_t XLXSWriter::WriteEscapedXML(const char *str) {
	return WriteEscapedXML(str, strlen(str));
}

inline idx_t XLXSWriter::WriteEscapedXML(const string &str) {
	return WriteEscapedXML(str.c_str(), str.size());
}

inline idx_t XLXSWriter::WriteEscapedXML(const char *buffer, idx_t write_size) {
	escaped_buffer.clear();
	EscapeXMLString(buffer, write_size, escaped_buffer);
	return stream.Write(escaped_buffer.data(), escaped_buffer.size());
}

}