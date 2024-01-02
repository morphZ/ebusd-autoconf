import socket
from csv import field_size_limit
from io import StringIO

import numpy as np
import pandas as pd

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 8888  # The port used by the server
FILE = "./ebusd-2.1.x/de/vaillant/08.hmu00.HW5103.csv"

COMMAND_COLS = 8
COMMAND_KEYS = ["type", "circuit", "name", "comment", "QQ", "ZZ", "PBSB", "ID"]

FIELD_COLS = 6
FIELD_NAMES = ["name", "ms", "datatypes_templates", "divider_values", "unit", "comment"]


def send_ebusd_tcp(command: str, host: str = HOST, port: int = PORT) -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(command.encode())

        buffer: bytes = b""
        while True:
            data = s.recv(1024)
            buffer += data
            if data.endswith(b"\n\n"):
                break

    return buffer.decode()


def load_config_tcp() -> pd.DataFrame:
    config = send_ebusd_tcp("find -f\n")
    num_cols = max([len(line.split(",")) for line in config.split("\n")])
    col_names = [f"{i}" for i in range(0, num_cols)]
    df = pd.read_csv(StringIO(config), sep=",", header=None, names=col_names, dtype=str)
    return df


def load_config_file(filename: str = FILE) -> pd.DataFrame:
    # %%
    raw = pd.read_csv(FILE, header=0)
    raw.columns.values[0] = "type_"
    raw = raw.loc[~raw.type_.str.startswith("#")]

    return raw


def build_message_data(message: pd.Series) -> dict:
    command = message.values[:COMMAND_COLS].tolist()
    command = {k: v for k, v in zip(COMMAND_KEYS, command)}
    fields = []
    for idx in range(COMMAND_COLS, len(message.values), FIELD_COLS):
        field = message.values[idx : idx + FIELD_COLS]
        if not all(pd.isnull(field)):
            fields.append(field.tolist())

    fields = pd.DataFrame(fields, columns=FIELD_NAMES)

    command.update({"num_fields": len(fields), "fields": fields})

    return command
