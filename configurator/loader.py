import socket

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 8888  # The port used by the server


def send_ebusd_tcp(command: str, host: str = HOST, port: int = PORT):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(command.encode())

        buffer: bytes = b""
        while True:
            data = s.recv(1024)
            buffer += data
            print(f"Received {data!r}")
            if data.endswith(b"\n\n"):
                break

        print(f"Received {buffer.decode()}")


if __name__ == "__main__":
    send_ebusd_tcp("find -f\n")
