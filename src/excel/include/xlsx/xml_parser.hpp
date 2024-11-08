#pragma once

#include "xlsx/zip_file.hpp"
#include "expat.h"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {


//-------------------------------------------------------------------
// XML Parser
//-------------------------------------------------------------------

enum class XMLParseResult {
	OK,
	SUSPENDED,
	ABORTED,
};

class XMLParser {
public:
	XMLParser();
	virtual ~XMLParser();
	XMLParseResult Parse(const char* buffer, idx_t len, bool final) const;
	XMLParseResult Resume() const;
	void ParseAll(ZipFileReader &stream, idx_t buffer_size = 2048) const;
protected:
	void EnableTextHandler(bool enable) const;
	void Stop(bool resumable) const;

	virtual void OnText(const char *text, idx_t len) { }
	virtual void OnStartElement(const char* name, const char** atts) = 0;
	virtual void OnEndElement(const char* name) = 0;

	static bool MatchTag(const char* tag, const char* name, bool strip_prefix = true);
private:
	XML_Parser parser;
};

inline XMLParser::XMLParser() : parser(XML_ParserCreate(nullptr)) {

	XML_SetUserData(parser, this);

	XML_SetStartElementHandler(parser, [](void *self_ptr, const XML_Char *name, const XML_Char **atts) {
		auto &self = *static_cast<XMLParser*>(self_ptr);
		self.OnStartElement(name, atts);
	});

	XML_SetEndElementHandler(parser, [](void *self_ptr, const XML_Char *name) {
		auto &self = *static_cast<XMLParser*>(self_ptr);
		self.OnEndElement(name);
	});
}

inline XMLParser::~XMLParser() {
	XML_ParserFree(parser);
}

inline XMLParseResult XMLParser::Parse(const char *buffer, const idx_t len, const bool final) const {
	const auto status = XML_Parse(parser, buffer, UnsafeNumericCast<int>(len), final);
	switch(status) {
	case XML_STATUS_ERROR:
		if(XML_GetErrorCode(parser) == XML_ERROR_ABORTED) {
			return XMLParseResult::ABORTED;
		} else {
			const auto row_num = XML_GetCurrentLineNumber(parser);
			const auto col_num = XML_GetCurrentColumnNumber(parser);
			const auto err_str = XML_ErrorString(XML_GetErrorCode(parser));
			throw IOException("XML parse error at line %d, column %d: %s", row_num, col_num, err_str);
		}
	case XML_STATUS_SUSPENDED:
		return XMLParseResult::SUSPENDED;
	case XML_STATUS_OK:
		return XMLParseResult::OK;
	default:
		throw InternalException("Unknown XML parse status");
	}
}

inline XMLParseResult XMLParser::Resume() const {
	const auto status = XML_ResumeParser(parser);
	switch(status) {
	case XML_STATUS_ERROR:
		if(XML_GetErrorCode(parser) == XML_ERROR_ABORTED) {
			return XMLParseResult::ABORTED;
		} else {
			const auto row_num = XML_GetCurrentLineNumber(parser);
			const auto col_num = XML_GetCurrentColumnNumber(parser);
			const auto err_str = XML_ErrorString(XML_GetErrorCode(parser));
			throw IOException("XML parse error at line %d, column %d: %s", row_num, col_num, err_str);
		}
	case XML_STATUS_SUSPENDED:
		return XMLParseResult::SUSPENDED;
	case XML_STATUS_OK:
		return XMLParseResult::OK;
	default:
		throw InternalException("Unknown XML parse status");
	}
}


inline void XMLParser::EnableTextHandler(const bool enable) const {
	if(enable) {
		XML_SetCharacterDataHandler(parser, [](void *self_ptr, const XML_Char *text, int len) {
			auto &self = *static_cast<XMLParser*>(self_ptr);
			self.OnText(text, len);
		});
	} else {
		XML_SetCharacterDataHandler(parser, nullptr);
	}
}

inline void XMLParser::Stop(bool resumable) const {
	XML_StopParser(parser, resumable);
}

inline void XMLParser::ParseAll(ZipFileReader &stream, const idx_t buffer_size) const {
	const auto buffer_handle = make_unsafe_uniq_array_uninitialized<char>(buffer_size);
	const auto buffer = buffer_handle.get();

	// Read the stream in chunks and parse it until done or cancelled, resuming if necessary
	while (!stream.IsDone()) {
		const auto read_size = stream.Read(buffer, buffer_size);
		auto status = Parse(buffer, read_size, stream.IsDone());
		while (status == XMLParseResult::SUSPENDED) {
			status = Resume();
		}
		if (status == XMLParseResult::ABORTED) {
			return;
		}
	}
}

inline bool XMLParser::MatchTag(const char *tag, const char *name, bool strip_prefix) {
	// Look for a colon
	if (strip_prefix) {
		const auto colon = strchr(name, ':');
		if (colon) {
			name = colon + 1;
		}
	}
	return strcmp(tag, name) == 0;
}


}