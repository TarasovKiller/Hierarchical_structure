import psycopg2
import json
import random
from typing import Optional


class DatabaseManager():
    def __init__(self, db_config: dict):
        try:
            self.conn = psycopg2.connect(**db_config)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Ошибка подключения:\n{e}")
            return None

    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.__del__()

    def _create_random_data(self) -> list:
        """
        Генерирует случайные данные для заполнения таблицы offices.
        """
        i = 0
        for office_index in range(100):
            print(f"{office_index}/100")
            i += 1
            office_id = i
            office_parent_id = None
            type = 1
            office_name = f"Офис_{random.randint(1, 10000000000)}"
            yield (office_id, office_parent_id, office_name, type)
            # yield  (office_id,office_parent_id,office_name,type,f"{office_id}")
            for department_index in range(random.randint(40, 80)):
                i += 1
                department_id = i
                parent_id = office_id
                name = f"Отдел_{random.randint(1, 10000000000)}"
                type = 2
                yield (department_id, parent_id, name, type)
                # yield  (department_id,parent_id,name,type,f"{office_id}/{department_id}")
                for staff_index in range(random.randint(30, 1000)):
                    i += 1
                    staff_id = i
                    parent_id = department_id
                    name = f"Сотрудник_{random.randint(1, 10000000000)}"
                    type = 3
                    staff_id = i
                    yield (staff_id, parent_id, name, type)
                    # yield  (staff_id,parent_id,name,type,f"{office_id}/{department_id}/{staff_id}")

    def import_json(self, filepath: str) -> None:
        """
        Импортирует данные из JSON-файла в таблицу offices.
        """
        def _get_data(filepath: str) -> list:
            with open(filepath, 'r', encoding='utf8') as f:
                data = json.load(f)
            for d in data:
                yield [v for v in d.values()]

        data = _get_data(filepath)
        self.cursor.executemany(
            "INSERT INTO offices (id, parent_id, name, type) VALUES (%s, %s, %s, %s)",
            data
        )
        self.conn.commit()

    def random_fill(self) -> None:
        """
        Заполняет таблицу offices случайными данными.
        """

        data = self._create_random_data()
        self.cursor.executemany(
            "INSERT INTO offices (id, parent_id, name, type) VALUES (%s, %s, %s, %s)",
            data
        )
        self.conn.commit()

    def make_data(self, filepath: Optional[str] = None) -> None:
        """
        Создает таблицу offices, если она не существует, и индекс, заполняет ее данными.
        Если передан путь к JSON-файлу, то данные будут импортированы из файла,
        иначе будут сгенерированы случайные данные.

        """
        try:
            self.cursor.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'offices')")
            table_exists = self.cursor.fetchone()[0]
            if not table_exists:
                self.cursor.execute("""
                    CREATE TABLE offices (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255),
                        parent_id INTEGER,
                        type INTEGER
                    );
                    CREATE INDEX idx_offices_type_parent_id ON offices (type,parent_id);
                """)
                self.conn.commit()
                print("База данных успешно создана")

            if filepath is not None:
                self.import_json(filepath)
            else:
                self.random_fill()
        except Exception as e:
            self.conn.rollback()
            print(f"Error: {e}")

    def _get_staffs_by_staff_id(self, staff_id: int) -> list:
        """
        Получает список сотрудников, работающих в офисе,
        в котором работает сотрудник с заданным staff_id.
        """
        try:
            sql = """
            WITH RECURSIVE ancestors AS (
                SELECT id, parent_id, type
                FROM offices
                WHERE id = %s
                UNION ALL
                SELECT offices.id, offices.parent_id, offices.type
                FROM offices
                JOIN ancestors ON ancestors.parent_id = offices.id
                ), descendant_department AS (
                SELECT id
                FROM offices
                WHERE type = 2 AND parent_id=(SELECT id FROM ancestors OFFSET 2)
                ),descendant_staff AS (
                SELECT name
                FROM offices
                WHERE type = 3 AND parent_id IN (SELECT id FROM descendant_department))

            SELECT name FROM descendant_staff
            """
            self.cursor.execute(sql, (staff_id,))
            staffs = self.cursor.fetchall()
            return staffs

        except Exception as e:
            self.conn.rollback()
            print(f"Error: {e}")

    def print_staffs(self, staff_id: int) -> None:
        """
        Выводит на экран список сотрудников, работающих в офисе,
        в котором работает сотрудник с заданным staff_id.
        """
        staff_list = self._get_staffs_by_staff_id(staff_id)
        print("\n".join(map(lambda x: x[0], (staff_list))))

    def delete_all_data(self) -> None:
        """
        Удаляет все данные из таблицы offices.
        """
        try:
            self.cursor.execute("DELETE FROM offices")
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error: {e}")


if __name__ == "__main__":

    db_config = {}
    db_config['database'] = input("Введите название базы данных: ")
    db_config['user'] = input("Введите имя пользователя: ")
    db_config['password'] = input("Введите пароль: ")
    db_config['host'] = input("Введите адрес хоста: ")
    db_config['port'] = input("Введите порт: ")

    with DatabaseManager(db_config) as db:
        while True:
            print("Выберите действие:\n" +
                  "1 - Создать таблицу и загрузить данные из файла\n" +
                  "2 - Создать таблицу и загрузить рандомные данные (придется ждать)\n" +
                  "3 - Получить всех сотрудников одного офиса\n" +
                  "4 - Удалить все данные\n" +
                  "0 - Выйти\n")

            choice = input("Ваш выбор:")

            match choice:
                case "1":
                    filepath = input("Введите путь к файлу:")
                    db.make_data(filepath)
                    print("Данные загружены")

                case "2":
                    db.make_data()
                    print("Данные загружены")

                case "3":
                    staff_id = input("Введите ID сотрудника:")
                    db.print_staffs(staff_id)
                    print()

                case "4":
                    db.delete_all_data()
                    print("Данные успешно удалены")

                case "0":
                    break

                case _:
                    print("Выберите номер из предложенного списка")
                    pass
