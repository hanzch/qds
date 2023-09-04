from db.source.base import DataSource as ds


def create_databases():
    with open('init.sql', 'r', encoding='utf-8') as file:
        sql = file.read()
    sql = sql.replace('\n', ' ')
    sql_commands = sql.split(';')

    for command in sql_commands:
        command = command.strip()
        if command:  # 确保 SQL 语句不为空
            ds._command_clickhouse(command)


def insert_test():
    with open('test_data.sql', 'r', encoding='utf-8') as file:
        sql = file.read()
    sql = sql.replace('\n', ' ')
    sql_commands = sql.split(';')

    for command in sql_commands:
        command = command.strip()
        if command:  # 确保 SQL 语句不为空
            ds._command_clickhouse(command)


def create_test():
    with open('test_data.sql', 'a', encoding='utf-8') as file:
        # 执行查询并获取结果
        result = ds._command_clickhouse('SELECT * FROM quant.codes LIMIT 10000')
        strs = ', '.join(result)
        ress = strs.split('\n')
        print(ress)

        for row in ress:
            insert_command = f"INSERT INTO quant.codes VALUES ({row});\n"
            file.write(insert_command)


if __name__ == '__main__':
    create_databases()
    insert_test()
    # create_test()
