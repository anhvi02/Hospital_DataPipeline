# replace BOOLEAN data type with BIT
def replace_list_of_sql_commands(list_of_commands, old_command, new_command):
    list_of_commands = [command.replace(old_command, new_command) for command in list_of_commands]
    return list_of_commands
