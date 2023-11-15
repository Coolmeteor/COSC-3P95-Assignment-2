import socket
import os
import gzip
import binascii
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor, BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
from opentelemetry.exporter.jaeger.thrift import JaegerExporter


# set up Probability (40%) sampler for the client
probability_sampler = TraceIdRatioBased(0.4)

# set the sampler onto the global tracer provider
trace.set_tracer_provider(TracerProvider(sampler=probability_sampler))
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

def send_file(file_path, server_address, use_compression = False, use_error_handling = False, use_checksum=False):
    with tracer.start_as_current_span("send_file"):
        # Get file content
        with open(file_path, 'r') as file:
            file_content = file.read()

        # Include file name in the content
        file_content = f"File Name: {os.path.basename(file_path)}\nFile Content: {file_content}"

        # Calculate and append checksum
        if use_checksum:
            checksum = binascii.crc32(file_content.encode())  # Calculate checksum
            file_content += f"\nChecksum: {checksum}"  # Append checksum to file content

        # Get file size after including file name in the content
        file_size = len(file_content.encode())

        # Compress the file_content
        if use_compression:
            compressed_content = gzip.compress(file_content.encode())
            file_size = len(compressed_content)  # Adjust file size for compressed content

        retries = 3  # Define the number of retries
        while retries > 0:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                    client_socket.connect(server_address)

                    # Send file size as bytes
                    client_socket.send(str(file_size).encode() + b":")

                    # Send file content
                    if use_compression:
                        client_socket.send(compressed_content)
                    else:
                        client_socket.send(file_content.encode())

                    print(f"[*] Sent file: {os.path.basename(file_path)}")
                    break  # Break the loop if successful

            except Exception as e:
                retries -= 1
                if retries == 0 or not use_error_handling:
                    print(f"[ERROR] Failed to send file: {e}")
                    break

                print(f"[RETRY] Retrying... Attempts left: {retries}")
                continue

def main():
    server_address = ('127.0.0.1', 12345)
    local_folder = "files"

    for filename in os.listdir(local_folder):
        file_path = os.path.join(local_folder, filename)
        # Here is where you turn on/off the advanced features, by changing boolean values.
        # use_compression value must be the same in server.py and client.py
        send_file(file_path, server_address, True, True, True)

if __name__ == "__main__":
    main()