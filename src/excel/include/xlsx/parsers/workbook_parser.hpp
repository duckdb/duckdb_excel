#pragma once

#include "xlsx/xml_parser.hpp"

namespace duckdb {

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
	void OnStartElement(const char *name, const char **atts) override;
	void OnEndElement(const char *name) override;

private:
	enum class State {
		START,
		WORKBOOK,
		SHEETS,
		SHEET,
	};
	State state = State::START;
	vector<pair<string, string>> sheets;
};

inline void WorkBookParser::OnStartElement(const char *name, const char **atts) {
	switch (state) {
	case State::START:
		if (MatchTag("workbook", name)) {
			state = State::WORKBOOK;
		}
		break;
	case State::WORKBOOK:
		if (MatchTag("sheets", name)) {
			state = State::SHEETS;
		}
		break;
	case State::SHEETS:
		if (MatchTag("sheet", name)) {
			state = State::SHEET;
			// Now extract attributes
			const char *sheet_name = nullptr;
			const char *sheet_ridx = nullptr;
			for (idx_t i = 0; atts[i]; i += 2) {
				if (strcmp(atts[i], "name") == 0) {
					sheet_name = atts[i + 1];
				} else if (strcmp(atts[i], "r:id") == 0) {
					sheet_ridx = atts[i + 1];
				}
			}
			if (sheet_name && sheet_ridx) {
				sheets.emplace_back(sheet_name, sheet_ridx);
			} else {
				throw InvalidInputException("Invalid sheet entry in workbook.xml");
			}
		}
		break;
	default:
		break;
	}
}

inline void WorkBookParser::OnEndElement(const char *name) {
	switch (state) {
	case State::SHEET:
		if (MatchTag("sheet", name)) {
			state = State::SHEETS;
		}
		break;
	case State::SHEETS:
		if (MatchTag("sheets", name)) {
			state = State::WORKBOOK;
		}
		break;
	case State::WORKBOOK:
		if (MatchTag("workbook", name)) {
			Stop(false);
		}
		break;
	default:
		break;
	}
}

} // namespace duckdb