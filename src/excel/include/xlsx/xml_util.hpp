#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

template <class T>
void EscapeXMLString(const char *ptr, const idx_t len, T &out) {
	out.clear();
	for (idx_t i = 0; i < len; i++) {
		auto c = ptr[i];
		switch (c) {
		case '\0':
			// Skip null characters, NUL is not allowed in XML anywhere
			break;
		case '&':
			out.insert(out.end(), {'&', 'a', 'm', 'p', ';'});
			break;
		case '<':
			out.insert(out.end(), {'&', 'l', 't', ';'});
			break;
		case '>':
			out.insert(out.end(), {'&', 'g', 't', ';'});
			break;
		case '"':
			out.insert(out.end(), {'&', 'q', 'u', 'o', 't', ';'});
			break;
		case '\'':
			out.insert(out.end(), {'&', 'a', 'p', 'o', 's', ';'});
			break;
		default:
			out.insert(out.end(), c);
			break;
		}
	}
}

inline string EscapeXMLString(const string &str) {
	string result;
	EscapeXMLString(str.c_str(), str.size(), result);
	return result;
}

} // namespace duckdb