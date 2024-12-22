# Problem - 1

For clarity and organization, my commits are stored in a separate repository. If you need access, please email me at hadysadegh@gmail.com, and I will grant you access.

## Running the Solution

To execute the solution, follow these steps:

0. Install Prerequisites
1. Clone this repository.
2. Start your MySQL server.
3. Update the `config.json` file with your MySQL server details:
   - **host**: MySQL server host
   - **user**: MySQL username
   - **password**: MySQL password
   - **port**: MySQL server port
4. Run the `bash.sh` script:
   - **Linux users**: You may need to edit `bash.sh` to replace `python` with `python3`.
5. The solution will be saved in shared/output directory

---

## Prerequisites

- `pandas`
- `mysql-connector-python`
- `python`
- `mysql`

---

## Configuring `config.json`

- **Database Connection**

  - `"database_name"`: Name of your database.
  - `"host"`: Host to connect to MySQL server.
  - `"user"`: Username to connect to MySQL server.
  - `"password"`: Password to connect to MySQL server.
  - `"port"`: Port to connect to MySQL server.

- **Directories**

  - `"input_data_dir"`: Directory of the input file.
  - `"output_dir"`: Directory for storing the result.
  - `"output_filename"`: Output file name with extension (e.g., `.json`).

- **Solution Choice**

  - `"solution"`: Choose `1` or `2` for the processing method. (more information about solutions at the end of the file)

- **SQL Source**

  - `"sql_source"`: Path to the SQL file containing database and stored procedure definitions.

- **Schema**
  - `"schema"`: [
    { "output_field": "uuid", "json_key": "article_uuid" },
    { "output_field": "title", "json_key": "article_title" },
    { "output_field": "abstract", "json_key": "article_abstract" },
    { "output_field": "tags", "json_key": "tags" },
    { "output_field": "relationships", "json_key": "relationships" }
    ]
- **Stored Procedure Names**
  - `"procedures"`: Names of stored procedures used in each step:
    - `"import_article"`: Procedure for inserting articles.
    - `"import_relationship"`: Procedure for inserting relationships.
    - `"import_tag"`: Procedure for inserting tags.
    - `"transform"`: Procedure for combining data (used only in Solution 1).
    - `"export"`: Procedure for exporting data (used only in Solution 1).
    - `"transform_export"`: Procedure for combining and exporting data in a single transaction (used only in Solution 2).

### Solution Descriptions

1. **Solution 1**:

   - Sequentially calls different procedures for each task within Python, handling errors as needed.
   - Steps:
     1. Call data import procedures.
     2. Call the data combination procedure.
     3. Call the data export procedure.

2. **Solution 2**:
   - Uses a single stored procedure to handle data combination and export together, using a transaction to manage errors.
   - Steps:
     1. Call data import procedures.
     2. Call a transform and export procedure (transaction handling).
