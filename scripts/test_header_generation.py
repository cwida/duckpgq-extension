import os
def generate_headers(base_dir):
    # Ensure to include the 'test' directory in the path
    test_dir = os.path.join(base_dir, "test")  # Adjust this if the base_dir does not already include 'test'

    for root, dirs, files in os.walk(test_dir):
        for file in files:
            if ".test" not in file:
                continue
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, base_dir)  # This assumes base_dir is the parent of 'test'
            group = "duckpgq"
            subpath = os.path.dirname(relative_path)
            if subpath:  # Check if there is a subdirectory path
                group += "_" + subpath.replace('/', '_').replace('test_', '')  # Correct the group to exclude 'test_'

            with open(file_path, 'r+') as f:
                content = f.read()

                # Create the header content
                header = f"# name: {relative_path}\n" \
                         f"# description: ""\n" \
                         f"# group: [{group}]\n\n"

                # Search and replace old header if exists
                if header.split("\n")[0] in content:
                    continue
                if content.startswith("# name:"):
                    end_of_header = content.find('\n\n') + 2
                    content = content[end_of_header:]  # Remove old header
                f.seek(0)
                f.write(header + content)  # Write new header and original content
                f.truncate()  # Truncate file in case new content is shorter than old


# Usage
base_dir = "/Users/dljtw/git/duckpgq"  # Adjust this path to the correct directory which includes the 'test' folder
generate_headers(base_dir)