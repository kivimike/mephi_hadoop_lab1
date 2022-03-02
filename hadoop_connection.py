from subprocess import Popen, PIPE
from datetime import datetime, timedelta
import random
import shlex
import os

# current time and date
# datetime object

programs = ['gnome', 'gimp', 'chrome', 'ubuntu', 'pycharm', 'hadoop', 'sqlite', 'konsole', 'telegram-desktop',
            'firefox',
            'system-monitor', 'libre-office', 'steam']
# messages = ['not respond', 'connection lost', 'running', 'successfully running', 'starts', 'permission denied',
#             'status: OK', 'status: DOWN', 'status: UP', 'call security', 'data was corrupted', 'never use md5']

messages = {
    0: ['Emergency: system is unusable', 'Process caused kernel crash', 'Process caused overheating. System down',
        'Process caused system fatal error'],
    1: ['Alert: process caused overheating. System is going to shut down'],
    2: ['Critical CPU load. 98% CPU load', 'Critical GPU load. 98% GPU load', 'Critical memory load. 98% memory load'],
    3: ['Error: program is not compatible wit your OS.', 'status: Down', "Not enough memory to run the program"],
    4: ['Warning: no internet connection. Displaying cashed information',
        'You are using an old version of the program. Update it for the future support'],
    5: ['New update available'],
    6: ['status: UP', 'status: BOOTING', 'status: FINISHED'],
    7: ['Debug: a', 'Debug: aa', 'Debug: bbb', 'Debug: ssss']}

wrong_logs = ['8,Jul 27 08:33:39,root,pycharm:,starts\n', '09879808,7908\n',
              '6,Jul 27 08:32:39,root,pycharm:,starts, hello\n']


def create_logs(filename):
    dt = datetime.now()
    with open(filename, 'a') as f:
        for i in range(1000000):
            if i % 100000 == 0:
                f.writelines(random.choice(wrong_logs))
            else:
                f.writelines(create_log_event(dt))
            dt += timedelta(minutes=random.randint(0, 5), seconds=random.randint(0, 60))


def create_log_event(dt):
    severity = random.randint(0, 7)
    log = f'{severity},{dt.strftime("%b %-d %H:%M:%S")},root,{random.choice(programs)}:,{random.choice(messages[severity])}\n'
    return log


if __name__ == '__main__':
    command = Popen(['hdfs', 'dfs', '-mkdir', '/user'], stdout=PIPE, stderr=PIPE)
    command.communicate()
    command = Popen(['hdfs', 'dfs', '-mkdir', '/user/kivimike'], stdout=PIPE, stderr=PIPE)
    command.communicate()
    filenames = ['logs0', 'logs1', 'logs2', 'logs3', 'logs4', 'logs5', 'logs6']
    print('Creating Files')
    for f in filenames:
        create_logs('input/' + f)

    command = Popen(['hdfs', 'dfs', '-put', 'input', 'input'], stdout=PIPE, stderr=PIPE)
    command.communicate()
    print('Files loaded into HDFS')
    print('Type: "snappy" if you need compressed output SequenceFile. Otherwise press any key')
    if input().lower() == 'snappy':
        #изменить путь к проекту lab1_133
        args = shlex.split(
            'yarn jar /home/kivimike/IdeaProjects/lab1_133/target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output-snappy snappy')
        command = Popen(args)
        command.communicate()
        print('Job Done')
        with open('hfds_out_snappy', 'w+') as f:
            command = Popen(['hdfs', 'dfs', '-text', 'output-snappy/part-r-00000'], stdout=f, stderr=PIPE)
            command.communicate()
    else:
        # изменить путь к проекту lab1_133
        args = shlex.split(
            'yarn jar /home/kivimike/IdeaProjects/lab1_133/target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output')
        command = Popen(args)
        command.communicate()
        print('Job Done')
        command = Popen(['hdfs', 'dfs', '-copyToLocal', 'output/part-r-00000', 'hfds_out'], stdout=PIPE, stderr=PIPE)
        command.communicate()