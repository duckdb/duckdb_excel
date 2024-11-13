#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"
#include "xlsx/read_xlsx.hpp"
#include "xlsx/zip_file.hpp"

#include <duckdb/common/exception/conversion_exception.hpp>

namespace duckdb {

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------
struct WriteXLSXData final : public TableFunctionData {
	vector<LogicalType> column_types;
	vector<string> column_names;

	string file_path;
	string sheet_name;
	bool header;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, CopyFunctionBindInput &input,
											 const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto data = make_uniq<WriteXLSXData>();

	// Find the header options
	const auto header_opt = input.info.options.find("header");
	if(header_opt != input.info.options.end()) {
		if(header_opt->second.size() != 1) {
			throw BinderException("Header option must be a single boolean value");
		}
		string error_msg;
		Value bool_val;
		if(!header_opt->second.back().DefaultTryCastAs(LogicalType::BOOLEAN, bool_val, &error_msg))
		{
			throw BinderException("Header option must be a single boolean value");
		}
		data->header = BooleanValue::Get(bool_val);
	} else {
		data->header = false;
	}

	// Find the sheet name option
	const auto sheet_name_opt = input.info.options.find("sheet");
	if(sheet_name_opt != input.info.options.end()) {
		if(sheet_name_opt->second.size() != 1) {
			throw BinderException("Sheet name option must be a single string value");
		}
		data->sheet_name = StringValue::Get(sheet_name_opt->second.back());
	} else {
		data->sheet_name = "Sheet1";
	}

	data->column_types = sql_types;
	data->column_names = names;
	data->file_path = input.info.file_path;

	return std::move(data);
}

//------------------------------------------------------------------------------
// Init Global
//------------------------------------------------------------------------------
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

struct GlobalWriteXLSXData final : public GlobalFunctionData {
	ZipFileWriter writer;
	idx_t sheet_row_idx;
	vector<string> sheet_column_names; // A1... Z1, AA1... ZZ1, etc.
	vector<string> sheet_column_types; // e.g. "str", "n", etc.
	idx_t column_count;

	DataChunk cast_chunk;

	GlobalWriteXLSXData(ClientContext &context, const string &file_path, const WriteXLSXData &data)
		: writer(context, file_path), sheet_row_idx(1), column_count(data.column_names.size()) {

		// Generate column names
		sheet_column_names.resize(column_count);
		for(idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			// Convert number to excel column name, e.g. 0 -> A, 1 -> B, 25 -> Z, 26 -> AA, etc.
			string col_name;
			idx_t col_num = col_idx + 1;
			while(col_num > 0) {
				col_name = (char)('A' + (col_num - 1) % 26) + col_name;
				col_num = (col_num - 1) / 26;
			}
			sheet_column_names[col_idx] = col_name;
		}

		// Generate column types
		sheet_column_types.resize(column_count);
		for(idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			// Convert the logical type to the excel cell type identifier
			const auto &type = data.column_types[col_idx];
			if(type.IsNumeric()) {
				sheet_column_types[col_idx] = "n";
			} else {
				sheet_column_types[col_idx] = "inlineStr";
			}
		}


		// Initialize the cast chunk;
		const vector<LogicalType> types(column_count, LogicalType::VARCHAR);
		cast_chunk.Initialize(BufferAllocator::Get(context), types);
	}
};

static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data,
															   const string &file_path) {
	auto &data = bind_data.Cast<WriteXLSXData>();
	auto gstate = make_uniq<GlobalWriteXLSXData>(context, file_path, data);

	// Create all the mandatory directories
	auto &writer = gstate->writer;
	writer.AddDirectory("_rels/");
	writer.AddDirectory("xl/");
	writer.AddDirectory("xl/_rels/");
	writer.AddDirectory("xl/worksheets/");

	writer.BeginFile("[Content_Types].xml");
	writer.Write(CONTENT_TYPES_XML);
	writer.EndFile();

	writer.BeginFile("xl/workbook.xml");
	writer.Write(WORKBOOK_XML_START);
	writer.Write(R"(<sheet state="visible" name=")");
	writer.WriteEscapedXML(data.sheet_name);
	writer.Write(R"(" sheetId="1" r:id="rId4"/>)");
	writer.Write(WORKBOOK_XML_END);
	writer.EndFile();

	writer.BeginFile("xl/_rels/workbook.xml.rels");
	writer.Write(WORKBOOK_REL_XML);
	writer.EndFile();

	// Begin writing the worksheet
	gstate->writer.BeginFile("xl/worksheets/sheet1.xml");
	gstate->writer.Write(WORKSHEET_XML_START);

	// Write the header
	if(data.header) {
		const auto row_ref = std::to_string(gstate->sheet_row_idx);
		gstate->writer.Write("<row r=\"" + row_ref + "\">");
		for(idx_t col_idx = 0; col_idx < gstate->column_count; col_idx++) {
			const auto col_ref = gstate->sheet_column_names[col_idx] + row_ref;
			gstate->writer.Write("<c r=\"" + col_ref + "\" t=\"inlineStr\">");
			gstate->writer.Write("<is><t>");
			gstate->writer.WriteEscapedXML(data.column_names[col_idx]);
			gstate->writer.Write("</t></is>");
			gstate->writer.Write("</c>");
		}
		gstate->writer.Write("</row>");
		gstate->sheet_row_idx++;
	}

	return std::move(gstate);
}

//------------------------------------------------------------------------------
// Init Local
//------------------------------------------------------------------------------
struct LocalWriteXLSXData final : public LocalFunctionData {
};

static unique_ptr<LocalFunctionData> InitLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq<LocalWriteXLSXData>();
}

//------------------------------------------------------------------------------
// Sink
//------------------------------------------------------------------------------
static void Sink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
						 LocalFunctionData &lstate, DataChunk &input) {
	auto &data = bind_data.Cast<WriteXLSXData>();
	auto &state = gstate.Cast<GlobalWriteXLSXData>();
	auto &writer = state.writer;

	const auto row_count = input.size();
	const auto col_count = input.data.size();

	// First, setup
	vector<UnifiedVectorFormat> formats;
	formats.resize(col_count);

	for(idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		auto &source_col = input.data[col_idx];
		auto &target_col = state.cast_chunk.data[col_idx];

		// Cast all the input columns to the target column
		VectorOperations::Cast(context.client, source_col, target_col, row_count);

		// Setup the unified format for the cast columns
		auto &format = formats[col_idx];
		target_col.ToUnifiedFormat(row_count, format);
	}

	// Now write the rows as xml
	for(idx_t in_idx = 0; in_idx < row_count; in_idx++) {
		const auto row_ref = std::to_string(state.sheet_row_idx);
		writer.Write("<row r=\"" + row_ref + "\">");

		for(idx_t col_idx = 0; col_idx < col_count; col_idx++) {
			const auto &format = formats[col_idx];
			const auto row_idx = format.sel->get_index(in_idx);

			writer.Write("<c r=\"");
			writer.Write(state.sheet_column_names[col_idx]);
			writer.Write(row_ref);
			writer.Write("\" t=\"");
			writer.Write(state.sheet_column_types[col_idx]);
			writer.Write("\"><v>");

			if(format.validity.RowIsValid(row_idx)) {
				const auto &val = UnifiedVectorFormat::GetData<string_t>(format)[row_idx];
				if(data.column_types[col_idx].IsNumeric()) {
					// Write numbers directly
					writer.WriteEscapedXML(val.GetData(), val.GetSize());
				} else {
					// Write as inline string
					writer.Write("<is><t>");
					writer.WriteEscapedXML(val.GetData(), val.GetSize());
					writer.Write("</t></is>");
				}
			}
			writer.Write("</v></c>");
		}

		writer.Write("</row>");
		state.sheet_row_idx++;
	}
}

//------------------------------------------------------------------------------
// Combine
//------------------------------------------------------------------------------
static void Combine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
							LocalFunctionData &lstate) {
	//auto &data = bind_data.Cast<WriteXLSXData>();
	//auto &state = gstate.Cast<GlobalWriteXLSXData>();
}

//------------------------------------------------------------------------------
// Finalize
//------------------------------------------------------------------------------
static void Finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &state = gstate.Cast<GlobalWriteXLSXData>();

	// Finish writing the worksheet
	state.writer.Write(WORKSHEET_XML_END);
	state.writer.EndFile();
	state.writer.Finalize();
}

//------------------------------------------------------------------------------
// Execution Mode
//------------------------------------------------------------------------------
CopyFunctionExecutionMode ExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	// TODO: Support parallel ordered batch writes
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

//------------------------------------------------------------------------------
// Copy From
//------------------------------------------------------------------------------
static void SetBooleanValue(named_parameter_map_t &params, const string &key, const vector<Value> &val) {
	static constexpr auto error_msg = "'%s' option must be standalone or a BOOLEAN value";
	if(val.size() > 1) {
		throw BinderException(error_msg, key);
	}
	if(val.size() == 1) {
		if(val.back().type() != LogicalType::BOOLEAN) {
			throw BinderException(error_msg, key);
		}
		params[key] = val.back();
	} else {
		params[key] = Value::BOOLEAN(true);
	}
}

static void SetVarcharValue(named_parameter_map_t &params, const string &key, const vector<Value> &val) {
	static constexpr auto error_msg = "'%s' option must be a single VARCHAR value";
	if(val.size() != 1) {
		throw BinderException(error_msg, key);
	}
	if(val.back().type() != LogicalType::VARCHAR) {
		throw BinderException(error_msg, key);
	}
	params[key] = val.back();
}

static void ParseCopyFromOptions(XLSXReadData &data, case_insensitive_map_t<vector<Value>> &options) {

	// Just make it really easy for us, extract everything into a named parameter map
	named_parameter_map_t named_parameters;

	for(auto &kv : options) {
		auto key = StringUtil::Lower(kv.first);
		auto &val = kv.second;
		if(key == "sheet") {
			SetVarcharValue(named_parameters, key, val);
		} else if (key == "header") {
			SetBooleanValue(named_parameters, key, val);
		} else if (key == "all_varchar") {
			SetBooleanValue(named_parameters, key, val);
		} else if (key == "ignore_errors") {
			SetBooleanValue(named_parameters, key, val);
		} else if(key == "range") {
			SetVarcharValue(named_parameters, key, val);
		} else if(key == "stop_at_empty") {
			SetBooleanValue(named_parameters, key, val);
		} else if (key == "empty_as_varchar") {
			SetBooleanValue(named_parameters, key, val);
		}
	}

	// Now just pass this to the table function data
	ReadXLSX::ParseOptions(data.options, named_parameters);
}

static unique_ptr<FunctionData> CopyFromBind(ClientContext &context, CopyInfo &info,
	vector<string> &expected_names, vector<LogicalType> &expected_types) {

	auto result = make_uniq<XLSXReadData>();
	result->file_path = info.file_path;

	// TODO: Parse options
	ParseCopyFromOptions(*result, info.options);

	ZipFileReader archive(context, info.file_path);
	ReadXLSX::ResolveSheet(result, archive);

	// Column count mismatch!
	if(expected_types.size() != result->return_types.size()) {
		string extended_error = "Table schema: ";
		for (idx_t col_idx = 0; col_idx < expected_types.size(); col_idx++) {
			if (col_idx > 0) {
				extended_error += ", ";
			}
			extended_error += expected_names[col_idx] + " " + expected_types[col_idx].ToString();
		}
		extended_error += "\nXLSX schema: ";
		for (idx_t col_idx = 0; col_idx < result->return_types.size(); col_idx++) {
			if (col_idx > 0) {
				extended_error += ", ";
			}
			extended_error += result->column_names[col_idx] + " " + result->return_types[col_idx].ToString();
		}
		extended_error += "\n\nPossible solutions:";
		extended_error += "\n* Manually specify which columns to insert using \"INSERT INTO tbl SELECT ... "
						  "FROM read_xlsx(...)\"";
		extended_error += "\n* Provide an explicit range option with the same width as the table schema using e.g.";
		extended_error += "\"COPY tbl FROM ... (FORMAT 'xlsx', range 'A1:Z10')\"";

		throw ConversionException(
			"Failed to read file(s) \"%s\" - column count mismatch: expected %d columns but found %d\n%s",
			result->file_path, expected_types.size(), result->return_types.size(), extended_error);
	}

	// Override the column names and types with the expected ones
	result->return_types = expected_types;
	result->column_names = expected_names;

	return std::move(result);
}

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------
void WriteXLSX::Register(DatabaseInstance &db) {
	CopyFunction info("xlsx");

	info.copy_to_bind = Bind;
	info.copy_to_initialize_global = InitGlobal;
	info.copy_to_initialize_local = InitLocal;
	info.copy_to_sink = Sink;
	info.copy_to_combine = Combine;
	info.copy_to_finalize = Finalize;
	info.execution_mode = ExecutionMode;

	info.copy_from_bind = CopyFromBind;
	info.copy_from_function = ReadXLSX::GetFunction();

	info.extension = "xlsx";
	ExtensionUtil::RegisterFunction(db, info);
}

}