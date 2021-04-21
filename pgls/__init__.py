#!/usr/bin/env python3

import asyncio
import sys
from dataclasses import dataclass
from typing import List, Optional

import asyncpg
import asyncpg.exceptions
import click
from colorama import Fore as F

# TODO:
# - indexes
# - (mat)views
# - type properties (VARCHAR(N), NUMERIC(X, Y), etc.)
# - constraints
# - permission denied errors
# - filters

# TODO: create a binary package
# TODO: deploy to github / PyPI


@dataclass
class Size:
    bytes: int

    def human(self) -> str:
        if self.bytes >= 1024 ** 4:
            power = 4
            suffix = "TiB"
        elif self.bytes >= 1024 ** 3:
            power = 3
            suffix = "GiB"
        elif self.bytes >= 1024 ** 2:
            power = 2
            suffix = "MiB"
        elif self.bytes >= 1024 ** 1:
            power = 1
            suffix = "KiB"
        else:
            power = 0
            suffix = "bytes"

        value = round(self.bytes / 1024 ** power)
        return f"{value} {suffix}"


@dataclass
class Count:
    count: int

    def human(self) -> str:
        if self.count >= 1000 ** 3:
            power = 3
            suffix = "kkk rows"
        elif self.count >= 1000 ** 2:
            power = 2
            suffix = "kk rows"
        elif self.count >= 1000 ** 1:
            power = 1
            suffix = "k rows"
        else:
            power = 0
            suffix = " rows"

        value = round(self.count / 1000 ** power)
        return f"{value}{suffix}"


@dataclass
class Field:
    name: str
    description: Optional[str]
    type: str
    default: Optional[str]
    nullable: bool

    def display(self, ident: int):
        nullable = f" {F.RED}(nullable){F.RESET}" if self.nullable else ""

        print(
            "  " * ident,
            f"• {F.GREEN}{self.name}{F.RESET} ",
            f"| {self.type}",
            nullable,
            sep="",
        )


@dataclass
class Table:
    schema: str
    name: str
    description: Optional[str]
    size: Size
    rows: Count
    fields: List[Field]

    def display(self, ident: int):
        print(
            "  " * ident,
            f"• {F.BLUE}{self.schema}.{self.name}{F.RESET} ",
            F.LIGHTWHITE_EX,
            f"({self.size.human()}, {self.rows.human()})",
            F.RESET,
            f" {F.LIGHTWHITE_EX}(table){F.RESET}",
            sep="",
        )

        if self.description:
            print(
                "  " * (ident + 1),
                F.LIGHTWHITE_EX,
                self.description,
                F.RESET,
                sep="",
            )
            print()

        for field in self.fields:
            field.display(ident=ident + 1)


@dataclass
class Database:
    name: str
    description: Optional[str]
    owner: str
    size: Size
    tables: List[Table]

    def display(self, ident: int = 0):
        print(
            "  " * ident,
            f"• {F.CYAN}{self.name}{F.RESET} ",
            F.LIGHTWHITE_EX,
            f"({self.size.human()})",
            F.RESET,
            f" {F.LIGHTWHITE_EX}(database){F.RESET}",
            sep="",
        )

        if self.description:
            print(
                "  " * (ident + 1),
                F.LIGHTWHITE_EX,
                self.description,
                F.RESET,
                sep="",
            )
            print()

        for table in self.tables:
            table.display(ident=ident + 1)


async def fetch_and_display_all(dsn, sort, tables, indexes, views, fields):
    async for database in fetch_databases(dsn, sort, (tables=="show"), (fields=="show")):
        database.display()


async def fetch_databases(base_dsn, sort, show_tables, show_fields):
    if sort == "name":
        order_by_expression = "db.datname"
    elif sort == "size":
        order_by_expression = "pg_database_size(db.datname) desc"
    else:
        raise NotImplementedError()

    connection = await asyncpg.connect(f"{base_dsn}/postgres")
    data = await connection.fetch(f"""
        select db.datname as name,
               role.rolname as owner,
               shdesc.description,
               pg_database_size(db.datname) as size

          from pg_database db

     left join pg_roles role
            on role.oid = db.datdba
     left join pg_shdescription shdesc
            on shdesc.objoid = db.oid

         where not datistemplate
           and datname <> 'postgres'
           and datallowconn

      order by {order_by_expression}
    """)

    for row in data:
        if show_tables:
            try:
                tables = await fetch_tables(base_dsn, row["name"], sort, show_fields)
            except asyncpg.exceptions.InsufficientPrivilegeError:
                pass
        else:
            tables = []

        yield Database(
            name=row["name"],
            description=row["description"],
            owner=row["owner"],
            size=Size(row["size"]),
            tables=tables,
        )

    await connection.close()


async def fetch_tables(base_dsn, database_name, sort, show_fields):
    if sort == "name":
        order_by_expression = "schemaname, tablename"
    elif sort == "size":
        order_by_expression = """pg_table_size('"' || schemaname || '"."' || tablename || '"') desc"""
    else:
        raise NotImplementedError()

    connection = await asyncpg.connect(f"{base_dsn}/{database_name}")

    data = await connection.fetch(f"""
        select schemaname as schema,
               tablename as name,
               shdesc.description as description,
               pg_table_size('"' || schemaname || '"."' || tablename || '"') as size,
               (
                 select reltuples::bigint
                   from pg_class
                  where oid = ('"' || schemaname || '"."' || tablename || '"')::regclass::oid
               ) as rows

          from pg_tables t

     left join pg_shdescription shdesc
            on shdesc.objoid = ('"' || schemaname || '"."' || tablename || '"')::regclass::oid

         where schemaname not in ('pg_catalog', 'information_schema')

      order by {order_by_expression}
    """)

    if show_fields:
        fields_data = await connection.fetch(f"""
            select table_schema as schema_name,
                   table_name as table_name,
                   column_name as name,
                   data_type as type,
                   column_default as default,
                   cast(is_nullable as boolean) as nullable

              from information_schema.columns

          order by ordinal_position
        """)
    else:
        fields_data = []

    tables = []
    for row in data:
        fields = await fetch_fields(fields_data, row["schema"], row["name"])

        tables.append(Table(
            schema=row["schema"],
            name=row["name"],
            description=row["description"],
            size=Size(row["size"]),
            rows=Count(row["rows"]),
            fields=fields,
        ))

    await connection.close()

    return tables


async def fetch_fields(prefetched_fields, schema_name, table_name):
    data = [
        row
        for row in prefetched_fields
        if all([
            row["schema_name"] == schema_name,
            row["table_name"] == table_name,
        ])
    ]

    fields = []
    for row in data:
        fields.append(Field(
            name=row["name"],
            description=None,
            type=row["type"],
            default=row["default"],
            nullable=row["nullable"],
        ))
    return fields


@click.command()
@click.option("--sort", default="name", type=click.Choice(["name", "size"]))
@click.option("--show-tables", "tables", flag_value="show", default=True)
@click.option("--hide-tables", "tables", flag_value="hide")
@click.option("--show-fields", "fields", flag_value="show", default=True)
@click.option("--hide-fields", "fields", flag_value="hide")
@click.option("--show-views", "views", flag_value="show", default=True)
@click.option("--hide-views", "views", flag_value="hide")
@click.option("--show-indexes", "indexes", flag_value="show", default=True)
@click.option("--hide-indexes", "indexes", flag_value="hide")
@click.argument("dsn")
def main(**kwargs):
    asyncio.run(fetch_and_display_all(**kwargs))


if __name__ == "__main__":
    main()
