# PostgreSQL Upgrades Manager (PUM)

<p>
  <img src="https://raw.githubusercontent.com/opengisch/pum/main/docs/docs/assets/pum.png" alt="PUM Logo" width="200"/>
</p>

## New version

This is the code of pum version 1.
You can find version 0.x documentation at https://github.com/opengisch/pum/tree/old-v1

## About

PUM is a robust database migration management tool designed to streamline the process of managing PostgreSQL database upgrades. Inspired by tools like FlywayDB and Liquibase, PUM leverages metadata tables to ensure seamless database versioning and migration.

Complete documentation available at https://opengisch.github.io/pum/

## Key Features

- **Command-line and Python Integration**: Use PUM as a standalone CLI tool or integrate it into your Python project.
- **Database Versioning**: Automatically manage database versioning with a metadata table.
- **Changelog Management**: Apply and track SQL delta files for database upgrades.
- **Database Comparison**: Compare two databases to identify differences in tables, columns, constraints, and more.
- **Backup and Restore**: Create and restore database backups with ease.

## Why PUM?

Managing database migrations in a Version Control System (VCS) can be challenging, especially for production databases. PUM simplifies this process by embedding version metadata directly into the database, enabling efficient tracking and application of migrations.

PUM was developed to address challenges in the [TEKSI](https://github.com/TESKI) project, an open-source GIS for network management based on [QGIS](http://qgis.org/fr/site/).
