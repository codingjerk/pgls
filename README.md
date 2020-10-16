# pgls

[![PyPI](https://img.shields.io/pypi/v/pgls?style=flat-square)](https://pypi.org/project/pgls/)
![License](https://img.shields.io/pypi/l/pgls?style=flat-square)

CLI utility to display postgres database information as a tree.

![Clickbait](https://raw.githubusercontent.com/codingjerk/pgls/master/assets/usage.png)

## Installation

```bash
pip install pgls
```

## Usage

### Basic example

```bash
pgls postgres://user:password@db.example.com
# shows all databases and all nested entities
```

### Sort by size

```bash
pgls --sort=size postgres://user:password@db.example.com
# shows heavier databases and tables first
```

### Hide some information (also this speeds up gathering)

```bash
pgls --hide-columns postgres://user:password@db.example.com
# shows databases and tables without it's columns

pgls --hide-tables --hide-views --hide-indexes postgres://user:password@db.example.com
# shows only databases
```
