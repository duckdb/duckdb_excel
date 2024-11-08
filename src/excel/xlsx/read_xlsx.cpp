#include "xlsx/read_xlsx.hpp"
#include "xlsx/xml_util.hpp"
#include "xlsx/xml_parser.hpp"
#include "xlsx/zip_file.hpp"
#include "xlsx/xlsx_parts.hpp"

#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

static const auto WBOOK_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml";
static const auto SHEET_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml";

//-------------------------------------------------------------------
// "[Content_Types].xml" Parser
//-------------------------------------------------------------------
struct ContentInfo {
	string wbook_path;
	string sheet_path;
};

class ContentParser final : public XMLParser {
public:
	static ContentInfo ParseContentTypes(ZipFileReader &stream) {
		ContentParser parser;
		parser.ParseAll(stream);
		return std::move(parser.info);
	}
protected:
	void OnStartElement(const char *name, const char **atts) override {
		switch(state) {
		case State::START:
			if(MatchTag("Types", name)) {
				state = State::TYPES;
			} break;
		case State::TYPES:
			if(MatchTag("Override", name)) {
				state = State::OVERRIDE;

				// Now, also extract the attributes
				const char *ctype = nullptr;
				const char *pname = nullptr;

				for(idx_t i = 0; atts[i]; i += 2) {
					if(strcmp(atts[i], "ContentType") == 0) {
						ctype = atts[i + 1];
					} else if(strcmp(atts[i], "PartName") == 0) {
						pname = atts[i + 1];
					}
				}

				if(ctype && pname) {
					if(strcmp(ctype, WBOOK_CONTENT_TYPE) == 0) {
						info.wbook_path = pname;
					}
					else if(strcmp(ctype, SHEET_CONTENT_TYPE) == 0) {
						info.sheet_path = pname;
					}
				} else {
					throw InvalidInputException("Invalid content type entry in [Content_Types].xml");
				}

			} break;
		default:
			break;
		}
	}
	void OnEndElement(const char *name) override {
		switch(state) {
		case State::OVERRIDE:
			if(MatchTag("Override", name)) {
				state = State::TYPES;
			} break;
		case State::TYPES:
			if(MatchTag("Types", name)) {
				state = State::END;
				Stop(false);
			} break;
		default: break;
		}
	}
private:
	enum class State : uint8_t { START, TYPES, OVERRIDE, END };
	ContentInfo info;
	State state = State::START;
};

//-------------------------------------------------------------------
// "xl/workbook.xml" Parser
//-------------------------------------------------------------------

class WorkBookParser final : public XMLParser {
public:
	static vector<pair<string, string>> GetSheets(ZipFileReader &stream) {
		WorkBookParser parser;
		parser.ParseAll(stream);
		return std::move(parser.sheets);
	}
private:
	void OnStartElement(const char *name, const char **atts) override {
		switch(state) {
		case State::START:
			if(MatchTag("workbook", name)) {
				state = State::WORKBOOK;
			} break;
		case State::WORKBOOK:
			if(MatchTag("sheets", name)) {
				state = State::SHEETS;
			} break;
		case State::SHEETS:
			if(MatchTag("sheet", name)) {
				state = State::SHEET;
				// Now extract attributes
				const char *sheet_name = nullptr;
				const char *sheet_ridx = nullptr;
				for(idx_t i = 0; atts[i]; i += 2) {
					if(strcmp(atts[i], "name") == 0) {
						sheet_name = atts[i + 1];
					} else if(strcmp(atts[i], "r:id") == 0) {
						sheet_ridx = atts[i + 1];
					}
				}
				if(sheet_name && sheet_ridx) {
					sheets.emplace_back(sheet_name, sheet_ridx);
				} else {
					throw InvalidInputException("Invalid sheet entry in workbook.xml");
				}
			} break;
		default:
			break;
 		}
	}
	void OnEndElement(const char *name) override {
		switch(state) {
		case State::SHEET:
			if(MatchTag("sheet", name)) {
				state = State::SHEETS;
			} break;
		case State::SHEETS:
			if(MatchTag("sheets", name)) {
				state = State::WORKBOOK;
			} break;
		case State::WORKBOOK:
			if(MatchTag("workbook", name)) {
				Stop(false);
			} break;
		default:
			break;
		}
	}
private:
	enum class State { START, WORKBOOK, SHEETS, SHEET, };
	State state = State::START;
	vector<pair<string, string>> sheets;
};

//-------------------------------------------------------------------
// Relationship Parser
//-------------------------------------------------------------------

struct XLSXRelation {
	string id;
	string type;
	string target;
};

class RelParser final : public XMLParser {
public:
	static vector<XLSXRelation> ParseRelations(ZipFileReader &stream) {
		RelParser parser;
		parser.ParseAll(stream);
		return std::move(parser.relations);
	}

protected:
	void OnStartElement(const char *name, const char **atts) override {
		switch(state) {
		case State::START:
			if(MatchTag("Relationships", name)) {
				state = State::RELATIONSHIPS;
			}
			break;
		case State::RELATIONSHIPS:
			if(MatchTag("Relationship", name)) {
				state = State::RELATIONSHIP;

				// Extract the attributes
				const char *rid = nullptr;
				const char *rtype = nullptr;
				const char *rtarget = nullptr;

				for(idx_t i = 0; atts[i]; i += 2) {
					if(strcmp(atts[i], "Id") == 0) {
						rid = atts[i + 1];
					} else if(strcmp(atts[i], "Type") == 0) {
						rtype = atts[i + 1];
					} else if(strcmp(atts[i], "Target") == 0) {
						rtarget = atts[i + 1];
					}
				}

				if(rid && rtype && rtarget) {
					relations.emplace_back(XLSXRelation{rid, rtype, rtarget});
				} else {
					throw InvalidInputException("Invalid relationship entry in _rels/.rels");
				}
			}
			break;
		default:
			break;
		}
	}
	void OnEndElement(const char *name) override {
		switch(state) {
		case State::RELATIONSHIP:
			if(MatchTag("Relationship", name)) {
				state = State::RELATIONSHIPS;
			}
			break;
		case State::RELATIONSHIPS:
			if(MatchTag("Relationships", name)) {
				Stop(false);
			}
			break;
		default: break;
		}
	}
private:
	enum class State : uint8_t { START, RELATIONSHIPS, RELATIONSHIP };
	State state = State::START;

	vector<XLSXRelation> relations;
};

//-------------------------------------------------------------------
// StyleSheet Parser
//-------------------------------------------------------------------

class XLSXStyleParser final : public XMLParser {
public:
	unordered_map<idx_t, LogicalType> format_map;
	vector<LogicalType> style_formats;

	XLSXStyleParser() {
		style_formats.push_back(LogicalType::DOUBLE); // Default format (0)
		style_formats.push_back(LogicalType::DOUBLE); // Default format (1)
	}

protected:
	void OnStartElement(const char *name, const char **atts) override;
	void OnEndElement(const char *name) override;
private:
	enum class State : uint8_t { START, STYLESHEET, NUMFMTS, NUMFMT, CELLXFS, XF };
	State state = State::START;
};

template <class... ARGS>
static bool StringContainsAny(const char *str, ARGS &&...args) {
	for (auto &&substr : {args...}) {
		if (strstr(str, substr) != nullptr) {
			return true;
		}
	}
	return false;
}

void XLSXStyleParser::OnStartElement(const char *name, const char **atts) {
	switch(state) {
	case State::START:
		if(MatchTag("styleSheet", name)) {
			state = State::STYLESHEET;
		}
		break;
	case State::STYLESHEET:
		if(MatchTag("numFmts", name)) {
			state = State::NUMFMTS;
		} else if(MatchTag("cellXfs", name)) {
			state = State::CELLXFS;
		}
		break;
	case State::NUMFMTS: {
		state = State::NUMFMT;

		const char* id_ptr = nullptr;
		const char* format_ptr = nullptr;

		for(idx_t i = 0; atts[i]; i += 2) {
			if(strcmp(atts[i], "numFmtId") == 0) {
				id_ptr = atts[i + 1];
			} else if(strcmp(atts[i], "formatCode") == 0) {
				format_ptr = atts[i + 1];
			}
		}
		if(!id_ptr) {
			throw InvalidInputException("Invalid numFmt entry in styles.xml");
		}
		const auto id = strtol(id_ptr, nullptr, 10);
		if(id <= 163 || format_ptr == nullptr) {
			break;
		}

		const auto has_date_part = StringContainsAny(format_ptr, "DD", "dd", "YY", "yy");
		const auto has_time_part = StringContainsAny(format_ptr, "HH", "hh", "h", "H");

		if (has_date_part && has_time_part) {
			format_map.emplace(id, LogicalType::TIMESTAMP);
		} else if (has_date_part) {
			format_map.emplace(id, LogicalType::DATE);
		} else if (has_time_part) {
			format_map.emplace(id, LogicalType::TIME);
		} else {
			// If we dont know how to handle the format, default to the numeric value.
			format_map.emplace(id, LogicalType::DOUBLE); // TODO: Or double?
		}

	} break;
	case State::CELLXFS: {
		state = State::XF;
		const char* id_ptr = nullptr;

		for(idx_t i = 0; atts[i]; i += 2) {
			if(strcmp(atts[i], "numFmtId") == 0) {
				id_ptr = atts[i + 1];
			}
		}
		if(!id_ptr) {
			throw InvalidInputException("Invalid xf entry in styles.xml");
		}
		const auto id = strtol(id_ptr, nullptr, 10);
		if (id < 164) {
			// Special cases
			if (id >= 14 && id <= 17) {
				style_formats.push_back(LogicalType::DATE);
			} else if (id >= 18 && id <= 21) {
				style_formats.push_back(LogicalType::TIME);
			} else if (id == 22) {
				style_formats.push_back(LogicalType::TIMESTAMP);
			}
		} else {
			// Look up the ID in the format map
			const auto it = format_map.find(id);
			if (it != format_map.end()) {
				style_formats.push_back(it->second);
			}
		}
	} break;
	default:
		break;
	}
}

void XLSXStyleParser::OnEndElement(const char *name) {
	switch(state) {
		case State::NUMFMT:
			if(MatchTag("numFmt", name)) {
				state = State::NUMFMTS;
			}
			break;
		case State::XF:
			if(MatchTag("xf", name)) {
				state = State::CELLXFS;
			}
			break;
		case State::NUMFMTS:
			if(MatchTag("numFmts", name)) {
				state = State::STYLESHEET;
			}
			break;
		case State::CELLXFS:
			if(MatchTag("cellXfs", name)) {
				state = State::STYLESHEET;
			}
			break;
		case State::STYLESHEET:
			if(MatchTag("styleSheet", name)) {
				Stop(false);
			}
			break;
		default:
			break;
	}
}

//-------------------------------------------------------------------
// String Table
//-------------------------------------------------------------------
// StringTable is a simple class that stores a unique set of strings
// in arena backed memory and allows fast access by index.

class StringTable {
public:
	explicit StringTable(Allocator &alloc) : arena(alloc) {}

	idx_t Add(const string_t &str);
	const string_t& Get(idx_t val) const;
	void Reserve(idx_t count);

private:
	ArenaAllocator			arena;
	string_map_t<idx_t>		table;
	vector<string_t>		index; // todo: make this a reference to the key in the map?
};

idx_t StringTable::Add(const string_t &str) {

	// Check if the string is already in the map
	const auto found = table.find(str);
	if(found != table.end()) {
		return found->second;
	}

	// Create a new entry
	const auto val = index.size();

	// If the string is inlined, just store it in the map and vector
	if(str.IsInlined()) {
		table[str] = val;
		index.push_back(str);
		return val;
	}

	// Otherwise copy the string to the arena
	const auto len = str.GetSize();
	const auto src = str.GetData();
	const auto dst = arena.Allocate(len);
	memcpy(dst, src, len);

	const auto key = string_t(const_char_ptr_cast(dst), len);
	table[key] = val;
	index.push_back(key);

	return val;
}

const string_t& StringTable::Get(const idx_t val) const {
	return index[val];
}

void StringTable::Reserve(const idx_t count) {
	table.reserve(count);
	index.reserve(count);
}

//-------------------------------------------------------------------
// String Table Parser
//-------------------------------------------------------------------
class SharedStringsParser : public XMLParser {
protected:
	virtual void OnUniqueCount(idx_t count) { }
	virtual void OnString(const vector<char> &str) = 0;
private:
	void OnStartElement(const char *name, const char **atts) override {
		switch(state) {
		case State::START:
			if(MatchTag("sst", name)) {
				state = State::SST;
				// Optionally look for the uniqueCount attributes
				// TODO: Do we also look for count?
				for(idx_t i = 0; atts[i]; i += 2) {
					if(strcmp(atts[i], "uniqueCount") == 0) {
						// TODO: Check that this succeeds!
						const auto unique_count = atoi(atts[i + 1]);
						OnUniqueCount(unique_count);
					}
				}
			}
			break;
		case State::SST:
			if(MatchTag("si", name)) {
				state = State::SI;
			}
			break;
		case State::SI:
			if(MatchTag("t", name)) {
				state = State::T;
				// Enable text handling
				EnableTextHandler(true);
			}
			break;
		case State::T:
			// TODO: Throw? We should never get here
			break;
		default:
			// TODO: Count garbage iterations where we dont find anything and throw if we get too far
			// to guard against corrupted files.
			// we're probably going to have to set that in the parser base class itself
		break;
		}
	}
	void OnEndElement(const char *name) override {
		switch(state) {
		case State::T:
			if(MatchTag("t", name)) {
				// Pass the string to the handler
				OnString(data);
				// Reset the buffer
				data.clear();
				// Disable text handling
				EnableTextHandler(false);
				state = State::SI;
			}
			break;
		case State::SI:
			if(MatchTag("si", name)) {
				state = State::SST;
			}
			break;
		case State::SST:
			if(MatchTag("sst", name)) {
				Stop(false);
			}
			break;
		default:
			break;
		}
	}
	void OnText(const char *text, const idx_t len) override {
		data.insert(data.end(), text, text + len);
	}
private:
	enum class State : uint8_t { START, SST, SI, T };
	State state = State::START;
	vector<char> data;
};

// Parses the string table for a specific set of strings
class StringTableSearcher final : public SharedStringsParser {
public:
	explicit StringTableSearcher(const vector<idx_t> &ids_p) : ids(ids_p) {
		std::sort(ids.begin(), ids.end());
	}

	const unordered_map<idx_t, string> &GetResult() const {
		return result;
	}

protected:
	void OnString(const vector<char> &str) override {
		if(current_idx >= ids.size()) {
			// We're done, no more strings to find
			Stop(false);
			return;
		}
		const auto &id = ids[current_idx];
		if(id == current_str) {
			result[id] = string(str.data(), str.size());
			current_idx++;
		}
		current_str++;
	}
private:
	idx_t current_idx = 0;
	idx_t current_str = 0;

	vector<idx_t> ids;
	unordered_map<idx_t, string> result;
};

// Parses the string table and populate it completely
class StringTableParser final : public SharedStringsParser {
public:
	static void ParseStringTable(ZipFileReader &stream, StringTable &table) {
		const StringTableParser parser(table);
		parser.ParseAll(stream);
	}
private:
	explicit StringTableParser(StringTable &table_p) : table(table_p) {
	}
protected:
	void OnString(const vector<char> &str) override {
		table.Add(string_t(str.data(), str.size()));
	}
	void OnUniqueCount(const idx_t count) override {
		table.Reserve(count);
	}
private:
	StringTable &table;
};

//-------------------------------------------------------------------
// SheetParser
//-------------------------------------------------------------------

class SheetParserBase : public XMLParser {
public:
	void OnText(const char *text, idx_t len) override;
	void OnStartElement(const char *name, const char **atts) override;
	void OnEndElement(const char *name) override;
protected:
	virtual void OnBeginRow(idx_t row_idx) {};
	virtual void OnEndRow(idx_t row_idx) {};
	virtual void OnCell(const XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) {}
private:
	enum class State : uint8_t { START, SHEETDATA, ROW, CELL, V, IS, T };
	State state = State::START;

	XLSXCellPos  cell_pos = {0, 0};
	XLSXCellType cell_type = XLSXCellType::NUMBER;
	vector<char> cell_data = {};
	idx_t		 cell_style = 0;
};

void SheetParserBase::OnText(const char *text, idx_t len) {
	if(cell_data.size() + len > XLSX_MAX_CELL_SIZE * 2) {
		// Something is obviously wrong, error out!
		throw InvalidInputException("XLSX: Cell data too large (is the file corrupted?)");
	}
	cell_data.insert(cell_data.end(), text, text + len);
}

void SheetParserBase::OnStartElement(const char *name, const char **atts) {
	if(state == State::START && MatchTag("sheetData", name)) {
		state = State::SHEETDATA;
	}
	else if(state == State::SHEETDATA && MatchTag("row", name)) {
		state = State::ROW;

		const char *rref_ptr = nullptr;
		for(idx_t i = 0; atts[i]; i += 2) {
			if(strcmp(atts[i], "r") == 0) {
				rref_ptr = atts[i + 1];
			}
		}
		// Default: Increment the row
		if(!rref_ptr) {
			cell_pos.row++;
		} else {
			// Else, jump to the row reference
			// TODO: Verify this
			cell_pos.row = strtol(rref_ptr, nullptr, 10);
		}

		OnBeginRow(cell_pos.row);
	}
	else if(state == State::ROW && MatchTag("c", name)) {
		state = State::CELL;

		// Reset the cell data
		cell_data.clear();

		// We're entering a cell. Parse the attributes
		const char* type_ptr = nullptr;
		const char* cref_ptr = nullptr;
		const char* style_ptr = nullptr;
		for(idx_t i = 0; atts[i]; i += 2) {
			if(strcmp(atts[i], "t") == 0) {
				type_ptr = atts[i + 1];
			} else if (strcmp(atts[i], "r") == 0) {
				cref_ptr = atts[i + 1];
			} else if (strcmp(atts[i], "s") == 0) {
				style_ptr = atts[i + 1];
			}
		}

		// Default: 0
		cell_style = style_ptr ? strtol(style_ptr, nullptr, 10) : 0;
		// Default: NUMBER
		cell_type = type_ptr ? ParseCellType(type_ptr) : XLSXCellType::NUMBER;
		// Default: next cell
		if(!cref_ptr) {
			cell_pos.col++;
		} else {
			XLSXCellPos cref;
			if(!cref.TryParse(cref_ptr)) {
				throw InvalidInputException("Invalid cell reference in sheet");
			}
			if(cref.row != cell_pos.row) {
				throw InvalidInputException("Cell reference does not match row reference in sheet");
			}
			cell_pos.col = cref.col;
		}
	}
	else if(state == State::CELL && MatchTag("v", name)) {
		state = State::V;
		EnableTextHandler(true);
	}
	else if(state == State::CELL && MatchTag("is", name)) {
		state = State::IS;
	}
	else if(state == State::IS && MatchTag("t", name)) {
		state = State::T;
		EnableTextHandler(true);
	}
}

void SheetParserBase::OnEndElement(const char *name) {
	if(state == State::SHEETDATA && MatchTag("sheetData", name)) {
		Stop(false);
	}
	else if(state == State::ROW && MatchTag("row", name)) {
		OnEndRow(cell_pos.row);
		state = State::SHEETDATA;
	}
	else if(state == State::CELL && MatchTag("c", name)) {
		OnCell(cell_pos, cell_type, cell_data, cell_style);
		state = State::ROW;
	}
	else if(state == State::V && MatchTag("v", name)) {
		state = State::CELL;
		EnableTextHandler(false);
	}
	else if(state == State::IS && MatchTag("is", name)) {
		state = State::CELL;
	}
	else if(state == State::T && MatchTag("t", name)) {
		state = State::IS;
		EnableTextHandler(false);
	}
}

//-------------------------------------------------------------------
// Range Sniffer
//-------------------------------------------------------------------
// TODO:
// Decide here:
// If the user has supplied a range:
//	 We should consider the WHOLE first row of the range when deciding if it is a header or no
//   If we didnt parse enough, we should pad the begining or end with empty strings
//   We should also pad the values with empty strings
//	 Then we inspect the header and values cells to determine the types
// If the user has not supplied a range:
//   We should use the first contiguos sequence of non-empty cells as the header, and use these as the valid range

// In essence, it might make sense to have two different sniffers for this purpose.
// Once we have the first row with the second sniffer, we can use that as the range for the first sniffer.

// Make BeginRow and BeginCell return boolean so that we can choose to skip the row/cell or not.


// The range sniffer is used to determine the range of the sheet to scan
class RangeSniffer final : public SheetParserBase {
public:
	XLSXCellRange GetRange() const;
private:
	void OnEndRow(idx_t row_idx) override;
	void OnCell(const XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) override;
private:
	// The range of the first consecutive non-empty row
	idx_t beg_col = 0;
	idx_t end_col = 0;
	idx_t beg_row = 0;

	// The total range of the sheet
	idx_t min_col = 0;
	idx_t max_col = 0;

	enum class State : uint8_t { EMPTY, FOUND, ENDED};
	State state = State::EMPTY;
};

XLSXCellRange RangeSniffer::GetRange() const {
	if(beg_row == 0) {
		// We didnt find any rows... return the whole sheet
		return XLSXCellRange(1, 1, XLSX_MAX_CELL_ROWS, XLSX_MAX_CELL_COLS);
	}
	// Otherwise, return the sniffed range
	return XLSXCellRange(beg_row, beg_col, XLSX_MAX_CELL_ROWS, end_col + 1);
}

void RangeSniffer::OnCell(const XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) {
	min_col = std::min(min_col, pos.col);
	max_col = std::max(max_col, pos.col);

	switch(state) {
		case State::EMPTY:
			if(!data.empty()) {
				state = State::FOUND;
				beg_col = pos.col;
				end_col = pos.col;
			}
		break;
		case State::FOUND:
			if(data.empty()) {
				state = State::ENDED;
			} else {
				end_col = pos.col;
			}
		break;
		case State::ENDED:
			// We're done
			break;
	}
}

void RangeSniffer::OnEndRow(const idx_t row_idx) {
	if(state == State::FOUND || state == State::ENDED) {
		// We found a row with data, between beg_col and end_col
		// We can now use this as the range for the sheet
		beg_row = row_idx;
		Stop(false);
	} else {
		// Reset, continue on to the next row
		state = State::EMPTY;
		beg_col = 0;
		end_col = 0;
	}
}

//-------------------------------------------------------------------
// Header Sniffer
//-------------------------------------------------------------------

enum class XLSXHeaderMode : uint8_t { NEVER, MAYBE, FORCE };

class HeaderSniffer final : public SheetParserBase {
public:
	HeaderSniffer(const XLSXCellRange &range_p, const XLSXHeaderMode header_mode_p)
		: range(range_p), header_mode(header_mode_p) { }

	const XLSXCellRange &GetRange() const { return range; }
	vector<XLSXCell> &GetColumnCells() { return column_cells; }
	vector<XLSXCell> &GetHeaderCells() { return header_cells; }

private:
	void OnBeginRow(idx_t row_idx) override;
	void OnEndRow(idx_t row_idx) override;
	void OnCell(const XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) override;
private:
	XLSXCellRange range;
	XLSXHeaderMode header_mode;

	bool first_row = true;
	idx_t last_col = 0;

	vector<XLSXCell> header_cells;
	vector<XLSXCell> column_cells;
};

void HeaderSniffer::OnBeginRow(const idx_t row_idx) {
	if(!range.ContainsRow(row_idx)) {
		return;
	}
	column_cells.clear();
	last_col = range.beg.col - 1;
}

void HeaderSniffer::OnCell(const XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) {
	if(!range.ContainsCol(pos.col)) {
		return;
	}

	// Now, add the cell to the data cells, but make sure to pad if needed.
	if(last_col + 1 < pos.col) {
		// Pad with empty cells
		for(idx_t i = last_col + 1; i < pos.col; i++) {
			column_cells.emplace_back(XLSXCellType::NUMBER, XLSXCellPos(pos.row, i), "", 0);
		}
	}

	// Add the cell
	column_cells.emplace_back(type, pos, string(data.data(), data.size()), style);
	last_col = pos.col;
}


void HeaderSniffer::OnEndRow(const idx_t row_idx) {
	if(!range.ContainsRow(row_idx)) {
		column_cells.clear();
		last_col = range.beg.col - 1;
		return;
	}

	// If there are columns missing at the end, pad with empty cells
	if(last_col + 1 < range.end.col) {
		for(idx_t i = last_col + 1; i < range.end.col; i++) {
			column_cells.emplace_back(XLSXCellType::NUMBER, XLSXCellPos(row_idx, i), "", 0);
		}
	}

	// Now we have all the cells in the row, we can inspect them
	if(!first_row) {
		// This is the data row. We can stop here
		Stop(false);
		return;
	}

	// Now its time to determine the header row
	auto has_header = false;
	switch(header_mode) {
		case XLSXHeaderMode::NEVER:
			// We're not looking for a header, so we're done
			has_header = false;
			break;
		case XLSXHeaderMode::FORCE:
			has_header = true;
			break;
		case XLSXHeaderMode::MAYBE: {
			// We're looking for a header, but we're not sure if we found it yet
			// We need to inspect the cells to determine if this is a header row or not
			// We need all the rows to be non empty and strings
			auto all_strings = true;
			for(const auto &cell : column_cells) {
				auto &type = cell.type;
				const auto is_str = type == XLSXCellType::SHARED_STRING || type == XLSXCellType::INLINE_STRING;
				const auto is_empty = cell.data.empty();
				if(!is_str || is_empty) {
					all_strings = false;
					break;
				}
			}
			has_header = all_strings;
		}
	}

	if(!has_header) {
		// Generate a dummy header
		header_cells = column_cells;
		for(auto &cell : header_cells) {
			cell.type = XLSXCellType::INLINE_STRING;
			cell.style = 0;
			cell.data = cell.cell.ToString();
		}
		Stop(false);
		return;
	}

	// Save the header cells
	header_cells = column_cells;
	column_cells.clear();
	last_col = range.beg.col - 1;

	// Try to parse another row to see if we can find the data row
	first_row = false;

	// Move the range down one row
	range.beg.row = row_idx + 1;
}


/*
// Either the user supplies a range, and we treat the WHOLE range as a table
// Or, the user does not supply a range, and we try to infer it ourselves.
class HeaderSniffer final : public SheetParser {
	// Detects the header row and types within a range
private:
	void OnBeginRow(idx_t row_idx) override;
	void OnEndRow(idx_t row_idx) override;
	void OnCell(const XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) override;
private:
	XLSXCellRange search_range;

	vector<XLSXCell> header_cells;
	vector<XLSXCell> values_cells;

	bool header_found = false;
};

void HeaderSniffer::OnBeginRow(idx_t row_idx) {
	if(search_range.ContainsRow(row_idx)) {
		// We're in the search range
	}
}

void HeaderSniffer::OnCell(const XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) {
	if(!search_range.ContainsPos(pos)) {
		return;
	}
	values_cells.emplace_back(type, pos, string(data.data(), data.size()), style);
}

void HeaderSniffer::OnEndRow(idx_t row_idx) {
	if(!search_range.ContainsRow(row_idx)) {
		return;
	}



	if(header_found) {
		// We've already found the header, so we're done
		Stop(false);
		return;
	}

	header_cells = values_cells;
}

*/


class SheetReader : public XMLParser {
protected:
	// Called when a cell has been parsed
	virtual void OnCell(XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) = 0;
	// Called when a row has been parsed
	virtual void OnRow(idx_t row_idx) = 0;
private:
	void OnStartElement(const char *name, const char **atts) override {
		switch (state) {
		case State::START:
			if(MatchTag("sheetData", name)) {
				state = State::SHEETDATA;
			}
			break;
		case State::SHEETDATA:
			if(MatchTag("row", name)) {
				state = State::ROW;

				const char *rref_ptr = nullptr;
				for(idx_t i = 0; atts[i]; i += 2) {
					if(strcmp(atts[i], "r") == 0) {
						rref_ptr = atts[i + 1];
					}
				}
				if(!rref_ptr) {
					//throw InvalidInputException("Invalid row reference in sheet");
					// Not all rows have a reference, so we can ignore this
					cell_pos.row++;
				} else {
					cell_pos.row = strtol(rref_ptr, nullptr, 10);
				}
				row_in_range = cell_range.ContainsRow(cell_pos.row);
			}
			break;
		case State::ROW:
			if(row_in_range && MatchTag("c", name)) {
				state = State::CELL;
				const char* type_ptr = nullptr;
				const char* cref_ptr = nullptr;
				const char* style_ptr = nullptr;
				for(idx_t i = 0; atts[i]; i += 2) {
					if(strcmp(atts[i], "t") == 0) {
						type_ptr = atts[i + 1];
					} else if (strcmp(atts[i], "r") == 0) {
						cref_ptr = atts[i + 1];
					} else if (strcmp(atts[i], "s") == 0) {
						style_ptr = atts[i + 1];
					}
				}
				if(cref_ptr) {
					if(!cell_pos.TryParse(cref_ptr)) {
						throw InvalidInputException("Invalid cell reference in sheet");
					}
				} else {
					// Not all cells have a reference, so just move to the next one
					cell_pos.col++;
				}
				cell_type = ParseCellType(type_ptr);
				cell_style = style_ptr ? strtol(style_ptr, nullptr, 10) : 0;
				col_in_range = cell_range.ContainsCol(cell_pos.col);
			}
			break;
		case State::CELL:
			if(col_in_range && MatchTag("v", name)) {
				state = State::V;
				EnableTextHandler(true);
				break;
			}
			if(col_in_range && MatchTag("is", name)) {
				state = State::IS;
				break;
			}
			break;
		case State::IS:
			if(MatchTag("t", name)) {
				state = State::T;
				EnableTextHandler(true);
			}
			break;
		default:
			break;
		}
	}

	void OnEndElement(const char *name) override {
		switch(state) {
			case State::V:
				if(MatchTag("v", name)) {
					EnableTextHandler(false);
					state = State::CELL;
				}
				break;
			case State::T:
				if(MatchTag("t", name)) {
					EnableTextHandler(false);
					state = State::IS;
				}
				break;
			case State::IS:
				if(MatchTag("is", name)) {
					state = State::CELL;
				}
				break;
			case State::CELL:
				if(MatchTag("c", name)) {
					state = State::ROW;

					if(col_in_range) {
						OnCell(cell_pos, cell_type, cell_data, cell_style);
						cell_data.clear();
					}
				}
				break;
			case State::ROW:
				if(MatchTag("row", name)) {
					state = State::SHEETDATA;

					if(row_in_range) {
						OnRow(cell_pos.row);
					}
				}
				break;
			case State::SHEETDATA:
				if(MatchTag("sheetData", name)) {
					Stop(false);
				}
				break;
			default:
				break;
		}
	}

	void OnText(const char *text, idx_t len) override {
		if(cell_data.size() + len > XLSX_MAX_CELL_SIZE * 2) {
			// Something is obviously wrong, error our!
			throw InvalidInputException("XLSX: Cell data too large (is the file corrupted?)");
		}

		cell_data.insert(cell_data.end(), text, text + len);
	}

	enum class State : uint8_t {
		START,
		SHEETDATA,
		ROW,
		CELL,
		V,
		IS,
		T,
	};

	State state = State::START;
	vector<char> cell_data = {};
	XLSXCellType cell_type = XLSXCellType::UNKNOWN;
	XLSXCellPos cell_pos = {0, 0};
	idx_t cell_style = 0;
	XLSXCellRange cell_range;

	bool row_in_range = false;
	bool col_in_range = false;

protected:
	explicit SheetReader(const XLSXCellRange &range) : cell_range(range) { }
};

//-------------------------------------------------------------------
// Sheet Parser
//-------------------------------------------------------------------

class SheetParser final : public SheetReader {
public:
	explicit SheetParser(ClientContext &context, const XLSXCellRange &range, const StringTable &table)
		: SheetReader(range), string_table(table) {

		// Initialize the chunk
		const vector<LogicalType> types(range.Width(), LogicalType::VARCHAR);
		chunk.Initialize(context, types);

		// Set the beginning column
		beg_col = range.beg.col;
		// Allocate the sheet row number mapping
		sheet_row_number = make_unsafe_uniq_array<idx_t>(STANDARD_VECTOR_SIZE);
	}

	DataChunk& GetChunk() { return chunk; }

	string GetCellName(idx_t chunk_row, idx_t chunk_col) const {
		// Get the cell name and row given a chunk row and column
		const auto sheet_row = sheet_row_number[chunk_row];
		const auto sheet_col = chunk_col + beg_col;

		const XLSXCellPos pos = {sheet_col, static_cast<idx_t>(sheet_row)};
		return pos.ToString();
	}

protected:
	void OnCell(XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) override;
	void OnRow(idx_t row_idx) override;
private:
	// Shared String Table
	const StringTable &string_table;
	// Mapping from chunk row to sheet row
	unsafe_unique_array<idx_t> sheet_row_number;
	// Current chunk
	DataChunk chunk;
	// Current row in the chunk
	idx_t out_index = 0;

	// The sheet column index of the first chunk column
	idx_t beg_col;
};

void SheetParser::OnCell(XLSXCellPos &pos, XLSXCellType type, vector<char> &data, idx_t style) {
	// Get the column data
	auto &vec = chunk.data[pos.col - beg_col];

	// Push the cell data to our chunk
	const auto ptr = FlatVector::GetData<string_t>(vec);

	if(type == XLSXCellType::SHARED_STRING) {
		// Push a null to the buffer so that the string is null-terminated
		data.push_back('\0');
		// Now we can use strtol to get the shared string index
		const auto ssi = std::strtol(data.data(), nullptr, 10);
		// Look up the string in the string table
		ptr[out_index] = string_table.Get(ssi);
	}
	else if(data.empty() && type != XLSXCellType::INLINE_STRING) {
		// If the cell is empty (and not a string), we wont be able to convert it
		// so just null it immediately
		FlatVector::SetNull(vec, out_index, true);
	} else {
		// Otherwise just pass along the call data, we will cast it later.
		ptr[out_index] = StringVector::AddString(vec, data.data(), data.size());
	}
}

void SheetParser::OnRow(idx_t row_idx) {
	// Map the chunk row to the sheet row
	sheet_row_number[out_index] = UnsafeNumericCast<int32_t>(row_idx);

	out_index++;
	chunk.SetCardinality(out_index);
	if(out_index == STANDARD_VECTOR_SIZE) {
		// We have filled up the chunk, yield!
		out_index = 0;
		Stop(true);
	}
}

//-------------------------------------------------------------------
// Meta
//-------------------------------------------------------------------

struct XLSXMeta {
	// Map sheet names to paths in the workbook
	unordered_map<string, string> sheets;
	string primary_sheet;
};

static XLSXMeta ParseXLSXFileMeta(ZipFileReader &reader) {
	XLSXMeta result;

	// Extract the content types to get the primary sheet
	if(!reader.TryOpenEntry("[Content_Types].xml")) {
		throw BinderException("No [Content_Types].xml found in xlsx file");
	}
	const auto ctypes = ContentParser::ParseContentTypes(reader);
	reader.CloseEntry();

	if(!reader.TryOpenEntry("xl/workbook.xml")) {
		throw BinderException("No xl/workbook.xml found in xlsx file");
	}
	const auto sheets = WorkBookParser::GetSheets(reader);
	reader.CloseEntry();

	if(!reader.TryOpenEntry("xl/_rels/workbook.xml.rels")) {
		throw BinderException("No xl/_rels/workbook.xml.rels found in xlsx file");
	}
	const auto wbrels = RelParser::ParseRelations(reader);
	reader.CloseEntry();

	// TODO: This might not actually be the primary sheet,
	// TODO: we should still inspect it to see if this is valid xlsx
	// we should maybe go by the order of the sheets in the workbook
	/*
	if(!ctypes.sheet_path.empty() && ctypes.sheet_path[0] == '/') {
		result.primary_sheet = ctypes.sheet_path.substr(1);
	} else {
		result.primary_sheet = ctypes.sheet_path;
	}
	*/

	// TODO: Detect if we have a shared string table

	// Resolve the sheet names to the paths
	// Start by mapping rid to sheet path
	unordered_map<string, string> rid_to_sheet_map;
	for(auto &rel : wbrels) {
		if(StringUtil::EndsWith(rel.type, "/worksheet")) {
			rid_to_sheet_map[rel.id] = rel.target;
		}
	}

	// Now map name to rid and rid to sheet path
	for(auto &sheet : sheets) {
		const auto found = rid_to_sheet_map.find(sheet.second);
		if(found != rid_to_sheet_map.end()) {

			// Normalize everything to absolute paths
			if(StringUtil::StartsWith(found->second, "/xl/")) {
				result.sheets[sheet.first] = found->second.substr(1);
			} else {
				result.sheets[sheet.first] = "xl/" + found->second;
			}

			// Set the first sheet we find as the primary sheet
			if(result.primary_sheet.empty()) {
				result.primary_sheet = sheet.first;
			}
		}
	}

	if(result.sheets.empty()) {
		throw BinderException("No sheets found in xlsx file (is the file corrupt?)");
	}

	return result;
}


static void ResolveColumnNames(vector<XLSXCell> &header_cells, ZipFileReader &archive) {

	vector<idx_t> shared_string_ids;
	vector<idx_t> shared_string_pos;

	for(idx_t i = 0; i < header_cells.size(); i++) {
		auto &cell = header_cells[i];
		if(cell.type == XLSXCellType::SHARED_STRING) {
			shared_string_ids.push_back(std::strtol(cell.data.c_str(), nullptr, 10));
			shared_string_pos.push_back(i);
		}
	}

	// There is nothing to resolve
	if(shared_string_ids.empty()) {
		return;
	}

	// Resolve the shared strings
	if(!archive.TryOpenEntry("xl/sharedStrings.xml")) {
		throw BinderException("No shared strings found in xlsx file");
	}
	StringTableSearcher searcher(shared_string_ids);
	searcher.ParseAll(archive);
	archive.CloseEntry();

	auto &shared_strings = searcher.GetResult();

	// Replace the shared strings with the resolved strings
	for(idx_t i = 0; i < shared_string_pos.size(); i++) {
		header_cells[shared_string_pos[i]].data = shared_strings.at(shared_string_ids[i]);
	}
}

//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
class XLSXFunctionData final : public TableFunctionData {
public:
	string	file_path;
	string	sheet_path;
	idx_t	sheet_fidx;

	vector<LogicalType> return_types;
	vector<XLSXCellType> source_types;

	bool ignore_errors = false;

	// The range of the content in the sheet
	XLSXCellRange content_range;

	// Format styles
	XLSXStyleSheet style_sheet;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
														  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<XLSXFunctionData>();

	// Get the file name
	const auto file_path = StringValue::Get(input.inputs[0]);

	// Open the archive
	ZipFileReader archive(context, file_path);

	//const auto entries = archive.ListEntries();
	const auto meta = ParseXLSXFileMeta(archive);

	result->file_path = file_path;

	// Check which sheet to use, default to the primary sheet
	auto sheet_opt = input.named_parameters.find("sheet");
	auto sheet_name = meta.primary_sheet;

	if(sheet_opt != input.named_parameters.end()) {
		// We need to escape all user-supplied strings when searching for them in the XML
		sheet_name = EscapeXMLString(StringValue::Get(sheet_opt->second));
	}
	const auto found = meta.sheets.find(sheet_name);
	if(found == meta.sheets.end()) {
		throw BinderException("Sheet '%s' not found in xlsx file", sheet_name);
	}
	result->sheet_path = found->second;

	// Get the header mode
	auto header_mode_opt = input.named_parameters.find("header");
	auto header_mode = XLSXHeaderMode::MAYBE;
	if(header_mode_opt != input.named_parameters.end()) {
		header_mode = BooleanValue::Get(header_mode_opt->second) ? XLSXHeaderMode::FORCE : XLSXHeaderMode::NEVER;
	}

	auto all_varchar_opt = input.named_parameters.find("all_varchar");
	bool all_varchar = false;
	if(all_varchar_opt != input.named_parameters.end()) {
		all_varchar = BooleanValue::Get(all_varchar_opt->second);
	}

	auto ignore_errors_opt = input.named_parameters.find("ignore_errors");
	if(ignore_errors_opt != input.named_parameters.end()) {
		result->ignore_errors = BooleanValue::Get(ignore_errors_opt->second);
	}

	auto range_opt = input.named_parameters.find("range");
	XLSXCellRange range;
	bool detect_range = true;
	if(range_opt != input.named_parameters.end()) {
		auto range_str = StringValue::Get(range_opt->second);
		if(!range.TryParse(range_str.c_str())) {
			throw BinderException("Invalid range '%s' specified", range_str);
		}
		if(!range.IsValid()) {
			throw BinderException("Invalid range '%s' specified", range_str);
		}

		// Make sure the range is inclusive of the last cell
		range.end.col++;
		range.end.row++;

		detect_range = false;
	}

	// Parse the styles (so we can handle dates)
	if(archive.TryOpenEntry("xl/styles.xml")) {
		XLSXStyleParser style_parser;
		style_parser.ParseAll(archive);
		result->style_sheet = XLSXStyleSheet(std::move(style_parser.style_formats));
		archive.CloseEntry();
	}

	if(detect_range) {
		if(!archive.TryOpenEntry(result->sheet_path)) {
			throw BinderException("Sheet '%s' not found in xlsx file", result->sheet_path);
		}
		RangeSniffer range_sniffer;
		range_sniffer.ParseAll(archive);
		archive.CloseEntry();

		range = range_sniffer.GetRange();
	}

	if(!archive.TryOpenEntry(result->sheet_path)) {
		throw BinderException("Sheet '%s' not found in xlsx file", result->sheet_path);
	}
	HeaderSniffer sniffer(range, header_mode);
	sniffer.ParseAll(archive);
	archive.CloseEntry();

	// This is the range of actual data in the sheet (header not included)
	result->content_range = sniffer.GetRange();

	auto &header_cells = sniffer.GetHeaderCells();
	auto &column_cells = sniffer.GetColumnCells();

	if(column_cells.empty()) {
		if(header_cells.empty()) {
			throw BinderException("No rows found in xlsx file");
		}
		// Else, we have a header row but no data rows
		// Users seem to expect this to work, so we allow it by creating an empty dummy row of numbers
		for(auto &cell : header_cells) {
			column_cells.emplace_back(XLSXCellType::NUMBER, cell.cell, "", 0);
		}
	}

	// Resolve any shared strings in the header
	ResolveColumnNames(header_cells, archive);

	// Set the return names
	for(auto &cell : header_cells) {
		names.push_back(cell.data);
	}

	// Convert excel types to duckdb types
	for(auto &cell : column_cells) {
		auto duckdb_type = cell.GetDuckDBType(all_varchar, result->style_sheet);

		return_types.push_back(duckdb_type);
		result->return_types.push_back(duckdb_type);
		result->source_types.push_back(cell.type);
	}

	return std::move(result);
}

//-------------------------------------------------------------------
// Global State
//-------------------------------------------------------------------
class XLSXGlobalState final : public GlobalTableFunctionState {
public:
	explicit XLSXGlobalState(ClientContext &context, const string &file_name, const XLSXCellRange &range)
		: archive(context, file_name), strings(BufferAllocator::Get(context)), parser(context, range, strings),
		buffer(make_unsafe_uniq_array_uninitialized<char>(BUFFER_SIZE)), cast_vec(LogicalType::DOUBLE) {}

	ZipFileReader archive;
	StringTable strings;
	SheetParser parser;
	unsafe_unique_array<char> buffer;

	XMLParseResult status = XMLParseResult::OK;

	string cast_err;
	Vector cast_vec;

	atomic<idx_t> stream_pos = {0};
	idx_t stream_len = 0;

	// 8kb buffer
	static constexpr auto BUFFER_SIZE = 8096;
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<XLSXFunctionData>();
	auto state = make_uniq<XLSXGlobalState>(context, data.file_path, data.content_range);

	// Check if there is a string table. If there is, extract it
	if(state->archive.TryOpenEntry("xl/sharedStrings.xml")) {
		StringTableParser::ParseStringTable(state->archive, state->strings);
		state->archive.CloseEntry();
	}

	// Open the main sheet for reading
	if(!state->archive.TryOpenEntry(data.sheet_path)) {
		// This should never happen, we've already checked this in the bind function
		throw InvalidInputException("Sheet '%s' not found in xlsx file", data.sheet_path);
	}

	// Set the progress counters
	state->stream_len = state->archive.GetEntryLen();
	state->stream_pos = 0;

	return std::move(state);
}

//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------

int64_t ExcelToEpoch(const double time) {
	static constexpr auto DAYS_BETWEEN_1900_AND_1970 = 25569;
	static constexpr auto SECONDS_PER_DAY = 86400;
	// Excel date is the number of days since 1900-01-01
	// We need to convert it to a date

	if(!(std::fabs(time) < 365.0 * 10000)) {
		// error!
		return 0;
	}
	auto secs = time * SECONDS_PER_DAY;
	if(std::fabs(secs - std::round(secs)) < 1e-3) {
		secs = std::round(secs);
	}

	return static_cast<int64_t>(secs) - static_cast<int64_t>(DAYS_BETWEEN_1900_AND_1970) * static_cast<int64_t>(SECONDS_PER_DAY);
}

static void TryCast(XLSXGlobalState &state, bool ignore_errors, const idx_t col_idx, ClientContext &context, Vector &target_col) {

	auto &chunk = state.parser.GetChunk();
	auto &source_col = chunk.data[col_idx];
	const auto row_count = chunk.size();

	const auto ok = VectorOperations::TryCast(context, source_col, target_col, row_count, &state.cast_err);
	if(!ok && !ignore_errors) {
		// Figure out which cell failed
		const auto &source_validity = FlatVector::Validity(source_col);
		const auto &target_validity = FlatVector::Validity(target_col);
		for(idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if(source_validity.RowIsValid(row_idx) != target_validity.RowIsValid(row_idx)) {
				const auto cell_name = state.parser.GetCellName(row_idx, col_idx);
				throw InvalidInputException("read_xlsx: Failed to parse cell '%s': %s", cell_name, state.cast_err);
			}
		}
	}
}

static void TryCastTime(XLSXGlobalState &state, bool ignore_errors, const idx_t col_idx, ClientContext &context, Vector &target_col) {
	// First cast it to a double
	TryCast(state, ignore_errors, col_idx, context, state.cast_vec);

	// Then convert the double to a time
	const auto row_count = state.parser.GetChunk().size();
	UnaryExecutor::Execute<double, dtime_t>(state.cast_vec, target_col, row_count, [&](const double &input) {
		const auto epoch = ExcelToEpoch(input);
		const auto stamp = Timestamp::FromEpochSeconds(epoch);
		return Timestamp::GetTime(stamp);
	});
}

static void TryCastDate(XLSXGlobalState &state, bool ignore_errors, const idx_t col_idx, ClientContext &context, Vector &target_col) {
	// First cast it to a double
	TryCast(state, ignore_errors, col_idx, context, state.cast_vec);

	// Then convert the double to a date
	const auto row_count = state.parser.GetChunk().size();
	UnaryExecutor::Execute<double, date_t>(state.cast_vec, target_col, row_count, [&](const double &input) {
		const auto epoch = ExcelToEpoch(input);
		const auto stamp = Timestamp::FromEpochSeconds(epoch);
		return Timestamp::GetDate(stamp);
	});
}

static void TryCastTimestamp(XLSXGlobalState &state, bool ignore_errors, const idx_t col_idx, ClientContext &context, Vector &target_col) {
	// First cast it to a double
	TryCast(state, ignore_errors, col_idx, context, state.cast_vec);

	// Then convert the double to a timestamp
	const auto row_count = state.parser.GetChunk().size();
	UnaryExecutor::Execute<double, timestamp_t>(state.cast_vec, target_col, row_count, [&](const double &input) {
		const auto epoch = ExcelToEpoch(input);
		return Timestamp::FromEpochSeconds(epoch);
	});
}

static void Execute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<XLSXFunctionData>();
	auto &gstate = data.global_state->Cast<XLSXGlobalState>();
	const auto buffer = gstate.buffer.get();

	auto &stream = gstate.archive;
	auto &parser = gstate.parser;
	auto &status = gstate.status;

	// Ready the chunk
	auto &chunk = parser.GetChunk();
	chunk.Reset();

	// Parse as much as we can until the parser yields (the chunk is full) or we run out of data
	while(!stream.IsDone()) {
		if(status == XMLParseResult::SUSPENDED) {
			// We need to resume parsing
			status = parser.Resume();
			if(status == XMLParseResult::SUSPENDED) {
				// Still suspended! Return the chunk
				break;
			}
		}

		// Otherwise, read more data
		const auto read_size = stream.Read(buffer, XLSXGlobalState::BUFFER_SIZE);

		// Update the progess
		gstate.stream_pos += read_size;

		status = parser.Parse(buffer, read_size, stream.IsDone());
		if(status == XMLParseResult::SUSPENDED) {
			// We need to return the chunk
			break;
		}
	}

	// Cast all the strings to the correct types, unless they are already strings in which case we reference them
	const auto row_count = chunk.size();

	for(idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &source_col = chunk.data[col_idx];
		auto &target_col = output.data[col_idx];
		auto &xlsx_type = bind_data.source_types[col_idx];

		const auto source_type = source_col.GetType().id();
		const auto target_type = target_col.GetType().id();

		if(source_type == target_type) {
			// If the types are the same, reference the column
			target_col.Reference(source_col);
		} else if(xlsx_type == XLSXCellType::NUMBER && target_type == LogicalTypeId::TIME) {
			TryCastTime(gstate, bind_data.ignore_errors, col_idx, context, target_col);
		} else if(xlsx_type == XLSXCellType::NUMBER && target_type == LogicalTypeId::DATE) {
			TryCastDate(gstate, bind_data.ignore_errors, col_idx, context, target_col);
		} else if(xlsx_type == XLSXCellType::NUMBER && target_type == LogicalTypeId::TIMESTAMP) {
			TryCastTimestamp(gstate, bind_data.ignore_errors, col_idx, context, target_col);
		} else {
			// Cast the from string to the target type
			TryCast(gstate, bind_data.ignore_errors, col_idx, context, target_col);
		}
	}
	output.SetCapacity(row_count);
	output.SetCardinality(row_count);

	output.Verify();
}

//-------------------------------------------------------------------
// Progress
//-------------------------------------------------------------------
static double Progress(ClientContext &context, const FunctionData *bind_data_p,
						 const GlobalTableFunctionState *global_state) {
	if (!global_state) {
		return 0;
	}

	const auto &state = global_state->Cast<XLSXGlobalState>();
	const auto pos = static_cast<double>(state.stream_pos.load());
	const auto len = static_cast<double>(state.stream_len);

	// Safety check
	if(pos == 0 || len == 0) {
		return 0;
	}

	return pos / len;
}

static unique_ptr<TableRef> XLSXReplacementScan(ClientContext &context, ReplacementScanInput &input,
										optional_ptr<ReplacementScanData> data) {
	const auto table_name = ReplacementScan::GetFullPath(input);
	const auto lower_name = StringUtil::Lower(table_name);

	if(!StringUtil::EndsWith(lower_name, ".xlsx")) {
		return nullptr;
	}

	auto result = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	result->function = make_uniq_base<ParsedExpression, FunctionExpression>("read_xlsx", std::move(children));

	return std::move(result);
}

//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
static TableFunction GetFunction() {

	TableFunction read_xlsx("read_xlsx", {LogicalType::VARCHAR}, Execute, Bind);
	read_xlsx.init_global = InitGlobal;
	read_xlsx.table_scan_progress = Progress;

	// Parameters
	read_xlsx.named_parameters["header"] = LogicalType::BOOLEAN;
	read_xlsx.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
	read_xlsx.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	read_xlsx.named_parameters["range"] = LogicalType::VARCHAR;
	read_xlsx.named_parameters["sheet"] = LogicalType::VARCHAR;

	return read_xlsx;
}

void ReadXLSX::Register(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, GetFunction());
	db.config.replacement_scans.emplace_back(XLSXReplacementScan);
}


} // namespace duckdb