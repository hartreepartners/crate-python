# -*- coding: utf-8; -*-
#
# Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

from .exceptions import ProgrammingError
from distutils.version import StrictVersion
import warnings

BULK_INSERT_MIN_VERSION = StrictVersion("0.42.0")

OFFSET_LIMIT_STRING = " limit {limit} offset {offset}"
DEFAULT_BATCH_SIZE = 10000

class Cursor(object):
    """
    not thread-safe by intention
    should not be shared between different threads
    """
    lastrowid = None  # currently not supported

    def __init__(self, connection, batch_size=None):
        self.arraysize = 1
        self.connection = connection
        self._closed = False
        self._result = None
        self.rows = None
        self.execute_stmt = None
        self.next_gen = None
        if batch_size == None:
            self.batch_size = DEFAULT_BATCH_SIZE
        else:
            self.batch_size = batch_size

    def __really_execute(self, sql, parameters=None, bulk_parameters=None):
        """
        Prepare and execute a database operation (query or command).
        """
        if self.connection._closed:
            raise ProgrammingError("Connection closed")

        if self._closed:
            raise ProgrammingError("Cursor closed")

        self._result = self.connection.client.sql(sql, parameters,
                                                  bulk_parameters)
        if "rows" in self._result and len(self._result["rows"]) > 0:
            return self._result["rows"]
    
    def __execute(self,sql, parameters=None, bulk_parameters=None):
        offset = 0
        while True:
            batched_sql = sql + OFFSET_LIMIT_STRING.format(limit=self.batch_size, offset=offset)
            rows = self.__really_execute(sql=batched_sql, parameters=parameters, bulk_parameters=bulk_parameters)
            if rows == None:
                break
            else:
                offset += len(rows) 
                yield rows
    
    def execute(self,sql, parameters=None, bulk_parameters=None):
        formatted_sql = sql.strip()
        formatted_lower_sql = formatted_sql.lower()
        if formatted_lower_sql[:6] == "select" and formatted_lower_sql.find("limit") == -1:
            if formatted_sql.endswith(";"):
                formatted_sql = formatted_sql[:-1]
            try:
                self.__really_execute(sql = formatted_sql + " limit 1", parameters=parameters, bulk_parameters=bulk_parameters)
                self.execute_stmt = self.__execute(sql=formatted_sql,parameters=parameters,bulk_parameters=bulk_parameters)
                self.next_gen = self.next()
                return
            except ProgrammingError:
                pass
        rows = self.__really_execute(sql = sql, parameters=parameters, bulk_parameters=bulk_parameters)
        if rows:
            self.next_gen = rows
        else:
            self.next_gen = []


    def executemany(self, sql, seq_of_parameters):
        """
        Prepare a database operation (query or command) and then execute it
        against all parameter sequences or mappings found in the sequence
        ``seq_of_parameters``.
        """
        row_counts = []
        durations = []
        if self.connection.lowest_server_version >= BULK_INSERT_MIN_VERSION:
            self.execute(sql, bulk_parameters=seq_of_parameters)
            for result in self._result.get('results', []):
                if result.get('rowcount') > -1:
                    row_counts.append(result.get('rowcount'))
            if self.duration > -1:
                durations.append(self.duration)
        else:
            for params in seq_of_parameters:
                self.execute(sql, parameters=params)
                if self.rowcount > -1:
                    row_counts.append(self.rowcount)
                if self.duration > -1:
                    durations.append(self.duration)
        self._result = {
            "rowcount": sum(row_counts) if row_counts else -1,
            "duration": sum(durations) if durations else -1,
            "rows": [],
            "cols": self._result.get("cols", []),
            "results": self._result.get("results")
        }
        self.rows = iter(self._result["rows"])
        return self._result["results"]

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.
        Alias for ``next()``.
        """
        try:
            return next(self.next_gen)
        except StopIteration:
            return None

    def __iter__(self):
        """
        support iterator interface:
        http://legacy.python.org/dev/peps/pep-0249/#iter

        This iterator is shared. Advancing this iterator will advance other
        iterators created from this cursor.
        """
        warnings.warn("DB-API extension cursor.__iter__() used")
        return self

    def fetchmany(self, count=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        if count is None:
            count = self.arraysize
        if count == 0:
            return self.fetchall()
        result = []
        for row in self.next_gen:
            result.append(row)
            if len(result) >= count:
                return result
        return result

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        result = []
        for row in self.next_gen:
            result.append(row)
        return result

    def close(self):
        """
        Close the cursor now
        """
        self._closed = True
        self._result = None

    def setinputsizes(self, sizes):
        """
        Not supported method.
        """
        pass

    def setoutputsize(self, size, column=None):
        """
        Not supported method.
        """
        pass

    @property
    def rowcount(self):
        """
        This read-only attribute specifies the number of rows that the last
        .execute*() produced (for DQL statements like ``SELECT``) or affected
        (for DML statements like ``UPDATE`` or ``INSERT``).
        """
        if (self._closed or not self._result or "rows" not in self._result):
            return -1
        return self._result.get("rowcount", -1)

    def next(self):
        """
        Return the next row of a query result set, respecting if cursor was
        closed.
        """
        for rows in self.execute_stmt:
            for row in rows:
                if self._closed:
                    raise ProgrammingError("Cursor closed")
                yield row

    __next__ = next

    @property
    def description(self):
        """
        This read-only attribute is a sequence of 7-item sequences.
        """
        if self._closed:
            return

        description = []
        for col in self._result["cols"]:
            description.append((col,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None))
        return tuple(description)

    @property
    def duration(self):
        """
        This read-only attribute specifies the server-side duration of a query
        in milliseconds.
        """
        if self._closed or \
                not self._result or \
                "duration" not in self._result:
            return -1
        return self._result.get("duration", 0)

