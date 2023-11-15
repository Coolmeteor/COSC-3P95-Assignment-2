import os
import random
import string

def generate_file(file_path, size_bytes):
    content = ''.join(random.choice(string.ascii_letters) for _ in range(size_bytes))
    with open(file_path, 'w') as file:
        file.write(content)

def generate_files(folder_path, num_files=20):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    for _ in range(num_files):
        size_kb = random.randint(5, 100) * 1024  # Size in bytes
        size_mb = random.randint(1, 100) * 1024 * 1024  # Size in bytes

        # Choose between generating a KB or MB file
        size_bytes = size_kb if random.choice([True, False]) else size_mb

        file_name = f"{size_bytes // 1024}KB_file.txt" if size_bytes < 1024 * 1024 else f"{size_bytes // (1024 * 1024)}MB_file.txt"
        file_path = os.path.join(folder_path, file_name)
        generate_file(file_path, size_bytes)

generate_files("files")