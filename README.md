# furball 🤮

ODBC driver for SQL Server. Powered by tabby. Nobody asked for this but here we are.

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

An ODBC driver that implements the ODBC API but uses [tabby](https://github.com/copycatdb/tabby) underneath instead of the Microsoft ODBC Driver. Why? Because sometimes your enterprise software only speaks ODBC and you have no choice. We understand. We dont judge.

## But why?

Look, we know. ODBC is the COBOL of database connectivity. Its been around since 1992 and refuses to die. But some things in this world require ODBC:

- Excel "Get Data"
- Crystal Reports (yes, people still use it)
- That one Java app from 2007 that nobody wants to touch
- Corporate policies written in stone tablets

So here we are. An ODBC driver. In Rust. Using a cat. To talk to SQL Server.

Were not proud. But were here.

## Status

🚧 Coming eventually. We need therapy first.

## Attribution

Inspired by [psqlODBC](https://github.com/postgresql-interfaces/psqlodbc) and the Microsoft ODBC Driver for SQL Server. Both have suffered so we dont have to. Mostly.

## License

MIT
