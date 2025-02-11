import nbformat

notebook_filename = './develop.ipynb'
output_filename = './extracted_code.py'


def extract_export_cells(notebook_path, output_path):

    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)

        for idx, cell in enumerate(nb.cells):
            print(f"{idx:-^100}")
            if cell.cell_type == 'markdown':
                print(cell)
            if cell.cell_type == 'code':
                tags = cell.metadata.get('tags', [])
                if 'export' in tags:
                    print("tags:", tags)
                print("type: ", type(cell.source))
                print(cell.source)

    # # Open the output file for writing
    # with open(output_path, 'w', encoding='utf-8') as out_file:
    #     for cell in nb.cells:
    #         # Only process code cells
    #         if cell.cell_type == 'code':
    #             # Check if the cell has metadata tags and if 'export' is among them
    #             tags = cell.metadata.get('tags', [])
    #             if 'export' in tags:
    #                 out_file.write(cell.source + "\n\n")


if __name__ == "__main__":
    extract_export_cells(notebook_filename, output_filename)
    print(f"Extracted code from '{notebook_filename}' to '{output_filename}'")
