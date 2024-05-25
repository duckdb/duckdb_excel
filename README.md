# DuckDB Excel Extension

This extension adds support for the [`TEXT` function](https://support.microsoft.com/en-us/office/text-function-20d5ac4d-7b94-49fd-bb38-93d29371225c) for formatting numbers from Microsoft Excel. 

Example usage:

```sql
SELECT text(1234567.897, '$#.##') AS result;
┌────────────┐
│   result   │
│  varchar   │
├────────────┤
│ $1234567.9 │
└────────────┘
```
