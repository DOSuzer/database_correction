import mysql.connector as mysql_connector


class DatabaseCorrection:
    """
    Класс для корректировки данных в продакшен БД по данным в тестовой БД.
    Допущения:
    - данные из тестовой БД не должны переносится в продакшен БД;
    - обе БД находятся на одном сервере (либо сделан дамп тестовой БД и загружен на сервер продакшн).
      В противном случае нужно создать два connection и оперировать ими;
    - не учтены множества нюансов из-за недостатка исходных данных
      (например невозможно однозначно сказать была ли таблица/столбец переименованны или удалены и вместо них созданы новые).
    """
    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 database_test,
                 database_prod):
        """Инициализация экземпляра класса."""
        self.connection = mysql_connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )
        self.database_test = database_test
        self.database_prod = database_prod

    def get_tables(self) -> tuple[list[str] | None, list[str] | None, list[str] | None]:
        """Получение новых и старых таблиц из БД."""
        with self.connection.cursor() as cursor:
            cursor.execute(f"SHOW TABLES FROM {self.database_test}")
            test_tables = cursor.fetchall()
            cursor.execute(f"SHOW TABLES FROM {self.database_prod}")
            prod_tables = cursor.fetchall()
            new_tables = list(item[0] for item in (set(test_tables) - set(prod_tables)))
            intersection = list(item[0] for item in (set(test_tables) & set(prod_tables)))
            delete_tables = list(item[0] for item in (set(prod_tables) - set(test_tables)))
            return new_tables, intersection, delete_tables

    def create_new_tables(self, tables: list[str]) -> None:
        """Создание новых таблиц в prod."""
        with self.connection.cursor() as cursor:
            create_query = "CREATE TABLE {}.{} LIKE {}.{};\n"
            for table in tables:
                cursor.execute(create_query.format(self.database_prod, table, self.database_test, table))
                self.connection.commit()

    def delete_tables(self, tables: list[str]) -> None:
        """Удаление таблиц в prod."""
        with self.connection.cursor() as cursor:
            create_query = "DROP TABLE {}.{};\n"
            for table in tables:
                cursor.execute(create_query.format(self.database_prod, table))
                self.connection.commit()

    def get_columns(self, table: str) -> tuple[list[str], list[str]]:
        """Получение списка колонок для таблицы из тестовой и продакшен баз данных."""
        query = ("SELECT group_concat(COLUMN_NAME) "
                 "FROM INFORMATION_SCHEMA.COLUMNS "
                 "WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}';")
        with self.connection.cursor() as cursor:
            cursor.execute(query.format(self.database_test, table))
            test_columns = cursor.fetchall()[0][0].split(',')
            cursor.execute(query.format(self.database_prod, table))
            prod_columns = cursor.fetchall()[0][0].split(',')
        return test_columns, prod_columns

    def rename_old_table(self, table: str) -> None:
        """Переименование старой таблицы."""
        with self.connection.cursor() as cursor:
            cursor.execute(f"USE {self.database_prod};")
            cursor.execute(f"ALTER TABLE {table} RENAME {table}_old;")
            self.connection.commit()

    def create_like_table(self, table: str) -> None:
        """Создание таблицы как в тестовой базе данных."""
        with self.connection.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {self.database_prod}.{table} "
                           f"LIKE {self.database_test}.{table};")
            self.connection.commit()

    def copy_data_to_new_table(self, table: str, matching_columns: str) -> None:
        """Копирование данных в новую таблицу."""
        with self.connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {self.database_prod}.{table} ({matching_columns}) "
                           f"SELECT {matching_columns} "
                           f"FROM {self.database_prod}.{table}_old;")
            self.connection.commit()

    def correct_table(self, table: str) -> None:
        """Корректировка таблицы в prod."""
        test_columns, prod_columns = self.get_columns(table)
        matching_columns = ", ".join([column for column in test_columns if column in prod_columns])
        self.rename_old_table(table)
        self.create_like_table(table)
        self.copy_data_to_new_table(table, matching_columns)

    def execute(self) -> str:
        """Выполнение корректировки БД prod."""
        new_tables, intersection, delete_tables = self.get_tables()
        for table in intersection:
            self.correct_table(table)
        if new_tables:
            self.create_new_tables(new_tables)
        if delete_tables:
            self.delete_tables(delete_tables)
        return "Done!"
