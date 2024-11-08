#pragma once

namespace duckdb {

class DatabaseInstance;

struct WriteXLSX {
  static void Register(DatabaseInstance &db);
};

struct ReadXLSX {
  static void Register(DatabaseInstance &db);
};

} // namespace duckdb