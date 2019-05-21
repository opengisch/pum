#!/usr/bin/env python

import psycopg2
from pum.core.deltapy import DeltaPy


class CreateViews(DeltaPy):

    def run(self):
        my_field_length = self.variables['my_field_length']

        conn = psycopg2.connect("service={0}".format(self.pg_service))
        cursor = conn.cursor()
        sql = "ALTER TABLE northwind.customers ALTER COLUMN country TYPE varchar({l});".format(l=my_field_length)
        cursor.execute(sql, self.variables)
        conn.commit()
        conn.close()


