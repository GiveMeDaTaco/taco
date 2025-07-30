import os
import argparse

# Define a delimiter that is unlikely to appear in a file path.
# This separates the metadata (path and size) from the file content.
METADATA_DELIMITER = "::"
HEADER_ENCODING = 'utf-8'


def pack_directory(source_dir, output_file):
    """
    Recursively packs a directory into a single file.

    For each file, it writes a header with metadata followed by the file's content.
    The header format is: relative_path::size\n

    Args:
        source_dir (str): The path to the directory to pack.
        output_file (str): The path to the single output file to create.
    """
    print(f"Starting to pack '{source_dir}' into '{output_file}'...")
    if not os.path.isdir(source_dir):
        print(f"Error: Source directory '{source_dir}' not found.")
        return

    try:
        with open(output_file, 'wb') as f_out:
            # os.walk provides all directories and files recursively.
            for dirpath, dirnames, filenames in os.walk(source_dir):
                # Handle empty directories to preserve the full structure.
                if not filenames and not dirnames:
                    relative_path = os.path.relpath(dirpath, source_dir)
                    # Use '.' for the root directory itself if it's empty.
                    if relative_path == ".":
                        continue
                    print(f"  - Archiving empty directory: {relative_path}")
                    header = f"{relative_path}{METADATA_DELIMITER}0\n"
                    f_out.write(header.encode(HEADER_ENCODING))

                # Handle files.
                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    relative_path = os.path.relpath(file_path, source_dir)
                    file_size = os.path.getsize(file_path)

                    print(f"  - Packing: {relative_path} ({file_size} bytes)")

                    # Create and write the header for the file.
                    header = f"{relative_path}{METADATA_DELIMITER}{file_size}\n"
                    f_out.write(header.encode(HEADER_ENCODING))

                    # Write the actual file content in binary.
                    with open(file_path, 'rb') as f_in:
                        f_out.write(f_in.read())
        print("Packing completed successfully.")
    except IOError as e:
        print(f"Error writing to file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during packing: {e}")


def unpack_directory(source_file, output_dir):
    """
    Unpacks a single file archive back into its original directory structure.

    It reads the archive sequentially, parsing headers to recreate directories
    and files.

    Args:
        source_file (str): The path to the single file archive.
        output_dir (str): The path to the directory where content will be extracted.
    """
    print(f"Starting to unpack '{source_file}' into '{output_dir}'...")
    if not os.path.isfile(source_file):
        print(f"Error: Source file '{source_file}' not found.")
        return

    # Create the root output directory if it doesn't exist.
    os.makedirs(output_dir, exist_ok=True)

    try:
        with open(source_file, 'rb') as f_in:
            while True:
                # Read the header line by line.
                header_line_bytes = f_in.readline()
                if not header_line_bytes:
                    break  # End of file

                header_line = header_line_bytes.decode(HEADER_ENCODING).strip()

                # Parse the header to get path and size.
                try:
                    relative_path, file_size_str = header_line.split(METADATA_DELIMITER)
                    file_size = int(file_size_str)
                except ValueError:
                    print(f"Error: Corrupted header found: {header_line}")
                    continue

                dest_path = os.path.join(output_dir, relative_path)

                # Ensure the parent directory for the file exists.
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                if file_size == 0:
                    # This handles empty files and empty directories.
                    # If it's an empty file, we create it.
                    # If it was an empty directory, os.makedirs already handled it.
                    if not os.path.isdir(dest_path):
                        open(dest_path, 'wb').close()
                    print(f"  - Created empty file/directory: {relative_path}")
                else:
                    # Read the file content and write it to the destination.
                    print(f"  - Unpacking: {relative_path} ({file_size} bytes)")
                    content = f_in.read(file_size)
                    if len(content) != file_size:
                        print("Error: Archive file is corrupted or incomplete.")
                        break
                    with open(dest_path, 'wb') as f_out:
                        f_out.write(content)
        print("Unpacking completed successfully.")
    except IOError as e:
        print(f"Error reading or writing file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during unpacking: {e}")


def main():
    """
    Main function to set up command-line argument parsing.
    """
    parser = argparse.ArgumentParser(
        description="A script to pack a directory into a single file and unpack it back."
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # --- Pack Command ---
    parser_pack = subparsers.add_parser("pack", help="Pack a directory into a single file.")
    parser_pack.add_argument(
        "-s", "--source", required=True, help="The source directory to pack."
    )
    parser_pack.add_argument(
        "-o", "--output", required=True, help="The path for the output archive file."
    )

    # --- Unpack Command ---
    parser_unpack = subparsers.add_parser("unpack", help="Unpack an archive file to a directory.")
    parser_unpack.add_argument(
        "-s", "--source", required=True, help="The source archive file to unpack."
    )
    parser_unpack.add_argument(
        "-o", "--output", required=True, help="The destination directory to unpack into."
    )

    args = parser.parse_args()

    if args.command == "pack":
        pack_directory(args.source, args.output)
    elif args.command == "unpack":
        unpack_directory(args.source, args.output)


if __name__ == "__main__":
    main()
