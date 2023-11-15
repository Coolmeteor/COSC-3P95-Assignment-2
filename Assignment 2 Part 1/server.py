import socket
import multiprocessing
import os
import gzip
import binascii
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor, BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
from opentelemetry.exporter.jaeger.thrift import JaegerExporter


# set up alwaysOn sampler for the server
alwaysOn_sampler = TraceIdRatioBased(1)

# set the sampler onto the global tracer provider
trace.set_tracer_provider(TracerProvider(sampler=alwaysOn_sampler))
tracer = trace.get_tracer(__name__)

# Use Jaeger exporter alongside Console exporter for local testing
jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",
    agent_port=6831,
)

# set up an exporter for sampled spans
trace.get_tracer_provider().add_span_processor(
    SimpleSpanProcessor(ConsoleSpanExporter())
)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(jaeger_exporter)
)

def handle_client(client_socket, data_folder, use_compression, use_error_handling, use_checksum):
    with tracer.start_as_current_span("handle_client"):
        # Receive file size as bytes until encountering ":"
        file_size_str = b""
        while True:
            byte = client_socket.recv(1)
            if byte == b":":
                break
            file_size_str += byte

        try:
            # Convert the received bytes to an integer
            file_size = int(file_size_str.decode())
        except ValueError:
            print("[ERROR] Invalid file size received.")
            return

        retries = 3  # Define the number of retries
        while retries > 0:
            try:
                # Receive file content
                if use_compression:
                    compressed_content = b""
                    while len(compressed_content) < file_size:
                        compressed_content += client_socket.recv(file_size - len(compressed_content))
                    file_content = gzip.decompress(compressed_content).decode()
                else:
                    file_content = client_socket.recv(file_size).decode()

                # Extract file name from content
                file_name = file_content.split('\n')[0].split(': ')[1]
                file_path = os.path.join(data_folder, file_name)

                # Verify data integrity with checksum
                if use_checksum:
                    # Split file content into lines to extract the appended checksum
                    content_lines = file_content.split('\n')
                    file_content_without_checksum = '\n'.join(content_lines[:-1])  # Exclude the last line (checksum)
                    received_checksum = int(content_lines[-1].split(': ')[1])  # Extract received checksum
                    calculated_checksum = binascii.crc32(file_content_without_checksum.encode())  # Calculate checksum
                    # Compare checksums to verify integrity
                    if received_checksum != calculated_checksum:
                        print("[ERROR] Data integrity check failed. File corrupted.")
                        return

                # Write file content to the server's data folder
                with open(file_path, 'w') as file:
                    file.write(file_content)

                print(f"[*] Received file: {file_name}")
                break  # Break the loop if successful

            except Exception as e:
                retries -= 1
                if retries == 0 or not use_error_handling:
                    print(f"[ERROR] Failed to receive file: {e}")
                    break

                print(f"[RETRY] Retrying... Attempts left: {retries}")
                continue

def start_server(data_folder, use_compression = False, use_error_handling = False, use_checksum = False):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', 12345))
    server.listen(5)

    print("[*] Server listening on port 12345")

    while True:
        client, addr = server.accept()
        print(f"[*] Accepted connection from {addr[0]}:{addr[1]}")

        client_handler = multiprocessing.Process(target=handle_client, args=(client, data_folder, use_compression, use_error_handling, use_checksum))
        client_handler.start()

if __name__ == "__main__":
    # Here is where you turn on/off the advanced features, by changing boolean values.
    # use_compression value must be the same in server.py and client.py
    start_server("server_files", True, True, True)