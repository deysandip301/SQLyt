# SQLyt

## Build

```bash
gcc -std=c11 -pthread main.c -o db
```

## Run

Default root folder is `./data`:

```bash
./db
```

Optional custom root folder:

```bash
./db /path/to/root
```

## Command Cheat Sheet

### Meta commands

1. Exit CLI

```text
.exit
```

2. Use an existing database (must already be created)

```text
.usedatabase <db_name>
```

3. List all database folders under root

```text
.showdatabases
```

4. List tables in active database with root pages

```text
.showtables
```

5. Print B+ tree structure for a table

```text
.btree <table_name>
```

6. Print storage constants

```text
.constants
```

### SQL commands

1. Create a database folder

```sql
create database app
```

2. Create a table
- first column must be `id int` (optionally `primary key`)
- remaining columns can be `int` or `text`
- `text` values are capped at **64 characters**; longer strings will be rejected

```sql
create table user (
  id int primary key,
  user_id int,
  user_name text,
  email text,
  city text
)
```

3. Insert a row
- first value is row key (`id`)
- remaining values must match schema order
- quoted strings are supported

```sql
insert into user values (1, 101, "Alice Doe", "alice@example.com", "New York")
```

4. Select all rows

```sql
select * from user
```

5. Delete a row

```sql
delete from user where id = 1
```

```sql
select * from user
```

## Quick Session Example

```text
create database app
.usedatabase app

create table user (id int primary key, user_id int, user_name text, email text)
insert into user values (1, 101, "Alice Doe", "alice@example.com")
insert into user values (2, 102, "Bob", "bob@example.com")

select * from user

delete from user where id = 1

select * from user

.showtables
.btree user
.exit
```
