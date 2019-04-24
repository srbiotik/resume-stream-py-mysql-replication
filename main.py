import os
from dotenv import load_dotenv
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent
)
from pymysqlreplication.event import (
    RotateEvent
)

load_dotenv()

mysql_settings = {'host': '127.0.0.1',
                  'port': 3306,
                  'user': 'root',
                  'passwd': 'kiloleba'}


resume_stream = os.getenv('resumeStream')
log_file, log_pos = ('mysql-bin.000122',
                     8243) if resume_stream else (None, None)

stream = BinLogStreamReader(
    connection_settings=mysql_settings,
    server_id=223344,
    resume_stream=True if resume_stream else False,
    log_file=log_file,
    log_pos=log_pos,
    only_events=(
        WriteRowsEvent,
        UpdateRowsEvent,
        RotateEvent,
    ),
    blocking=True
)

# CAn suffer from InternalError in case of max_eventlog_size is passed
# CAn suffer from ConnectionRefusedError if the server is unavailable
for binlogevent in stream:
    if issubclass(RotateEvent, type(binlogevent)):
        print("Rotating logs")
    else:
        print("Some other event", binlogevent.rows)

stream.close()
