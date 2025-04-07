#!/usr/bin/env python

import psycopg2

from pum.core.deltapy import DeltaPy


class CreateViews(DeltaPy):

    def run(self):
        my_field_length = self.variables["my_field_length"]

        conn = psycopg2.connect(f"service={self.pg_service}")
        cursor = conn.cursor()
        sql = f"ALTER TABLE northwind.customers ALTER COLUMN country TYPE varchar({my_field_length});"
        cursor.execute(sql, self.variables)
        conn.commit()
        conn.close()
