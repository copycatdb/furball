# furball 🐱

ODBC driver for SQL Server. Powered by [tabby](https://github.com/copycatdb/tabby).

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

An ODBC driver for SQL Server that uses tabby's Rust TDS implementation under the hood. Drop-in replacement for the Microsoft ODBC Driver — same ODBC API, different engine.

## Why ODBC?

ODBC is the universal adapter of the database world. It's been the standard since 1992 and it's still everywhere — not because nothing better came along, but because it *works*. Thirty years of tooling, integrations, and institutional knowledge don't just disappear.

ODBC connects:
- **Excel, Power BI, Tableau** — the tools analysts actually use every day
- **Enterprise ETL pipelines** — Informatica, SSIS, Talend, you name it
- **Every language ever** — Python (pyodbc), Java (JDBC-ODBC bridge), .NET, C/C++, Go, Ruby, Perl...
- **Legacy systems that run the world** — banks, hospitals, governments

The Microsoft ODBC Driver for SQL Server is excellent — rock solid, fully featured, regularly updated. furball isn't here to replace it. It's here to give the ODBC ecosystem another option: one built on tabby's modern Rust TDS stack, with no C/C++ dependencies, cross-compilable, and embeddable.

Think of it like how there are multiple PostgreSQL ODBC drivers (psqlODBC, the Devart one, etc.). More options = healthier ecosystem.

## Status

🚧 Coming soon. tabby handles the TDS protocol, furball wraps it in the ODBC C API.

## Attribution

Standing on the shoulders of [psqlODBC](https://github.com/postgresql-interfaces/psqlodbc) (the OG open-source ODBC driver) and the [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server) (the gold standard). Both paved the road we're driving on.

## License

MIT
