#pragma once

#include "xlsx/xml_parser.hpp"

namespace duckdb {

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
	void OnStartElement(const char *name, const char **atts) override;
	void OnEndElement(const char *name) override;
private:

	static constexpr auto WBOOK_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml";
	static constexpr auto SHEET_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml";

	enum class State : uint8_t { START, TYPES, OVERRIDE, END };
	ContentInfo info;
	State state = State::START;
};

inline void ContentParser::OnStartElement(const char *name, const char **atts) {
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

inline void ContentParser::OnEndElement(const char *name) {
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

} // namespace duckdb