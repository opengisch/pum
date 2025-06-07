# PostgreSQL Upgrades Manager (PUM)

![logo](https://raw.githubusercontent.com/opengisch/pum/main/docs/docs/assets/images/pum.png)

## New version

This is the code of pum version 1.
You can find version 0.x documentation at https://github.com/opengisch/pum/tree/old-v1

## About

PUM (PostgreSQL Upgrades Manager) is a robust database migration management tool designed to streamline the process of managing PostgreSQL database upgrades. Inspired by tools like FlywayDB and Liquibase, PUM leverages metadata tables to ensure seamless database versioning and migration.

## Key Features

- **Command-line and Python Integration**: Use PUM as a standalone CLI tool or integrate it into your Python project.
- **Database Versioning**: Automatically manage database versioning with a metadata table.
- **Changelog Management**: Apply and track SQL delta files for database upgrades.
- **Migration Hooks**: Define custom hooks to execute additional SQL or Python code before or after migrations. This feature allows you to isolate data (table) code from application code (such as views and triggers), ensuring a clear separation of concerns and more maintainable database structures.

## Why PUM?

Managing database migrations in a Version Control System (VCS) can be challenging, especially for production databases. PUM simplifies this process by embedding version metadata directly into the database, enabling efficient tracking and application of migrations.

PUM was developed to address challenges in the [TEKSI](https://github.com/TEKSI) project, an open-source GIS for network management based on [QGIS](http://qgis.org/).
