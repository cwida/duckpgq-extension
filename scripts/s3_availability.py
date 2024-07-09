import boto3

s3_client = boto3.client('s3')
bucket_name = 'duckpgq'
prefix = 'v'

def list_extensions(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    extensions = {}

    for obj in response.get('Contents', []):
        path_parts = obj['Key'].split('/')
        if len(path_parts) == 3:
            version = path_parts[0]
            os_arch = path_parts[1]
            parts = os_arch.split('_')
            if len(parts) > 2:
                os = parts[0]
                arch = '_'.join(parts[1:])
            else:
                os, arch = parts
            url = f'https://{bucket}.s3.eu-north-1.amazonaws.com/{obj["Key"]}'

            if version not in extensions:
                extensions[version] = {}
            if os not in extensions[version]:
                extensions[version][os] = []
            extensions[version][os].append((arch, url))

    return extensions

def generate_markdown_table(extensions):
    table = '## DuckPGQ Extension Availability\n\n'

    for version in sorted(extensions.keys(), reverse=True):
        os_dict = extensions[version]
        table += f'<details>\n<summary>Version {version}</summary>\n\n'
        for os, builds in os_dict.items():
            table += f'### {os.capitalize()}\n\n'
            table += '| Architecture | Download Link |\n'
            table += '|--------------|---------------|\n'
            for arch, url in builds:
                table += f'| {arch}        | [{os}_{arch}](<{url}>) |\n'
            table += '\n'
        table += '</details>\n\n'

    return table

extensions = list_extensions(bucket_name, prefix)
markdown_table = generate_markdown_table(extensions)

with open('extension_availability.md', 'w') as readme_file:
    readme_file.write(markdown_table)
