#pragma once
#include "xlsx/xml_parser.hpp"

namespace duckdb {

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
	void OnStartElement(const char *name, const char **atts) override;
	void OnEndElement(const char *name) override;
private:
	enum class State : uint8_t { START, RELATIONSHIPS, RELATIONSHIP };
	State state = State::START;
	vector<XLSXRelation> relations;
};

inline void RelParser::OnStartElement(const char *name, const char **atts) {
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

inline void RelParser::OnEndElement(const char *name) {
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


} // namespace duckdb