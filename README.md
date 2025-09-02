# QuixDB - Lightweight File-Based Database

QuixDB is a lightweight, file-based database system inspired by SQLite, implemented purely in Python. It provides basic SQL-like operations without requiring external dependencies, making it ideal for small projects and learning purposes.

## Features

- **Table Creation**: Define tables with typed columns and unique constraints
- **CRUD Operations**: Insert, select, update, and delete records
- **Transaction Safety**: File-based locking for concurrent access safety
- **Unique Constraints**: Enforce uniqueness on specified columns
- **Indexing**: Automatic indexing of unique columns for fast lookups
- **JSON Storage**: Human-readable data storage format

## How It Works

QuixDB uses a directory structure where:
- Each table is a folder
- Metadata is stored in `meta.json`
- Each row is stored as a separate JSON file
- Unique constraints are maintained using SHA-256 hash indexes
- File-based locking ensures transaction safety

### Storage Structure
database_folder/
  └── table_name/
  ├── meta.json
  ├── rows/
  │   └── [id].json
  └── index_[column_name]/
  └── [sha256_hash]


## Installation

Simply copy `QuixDB.py` into your project directory.

## Usage

### Basic Operations

```python
import QuixDB

DB_FOLDER = "UserData" # Define the folder where the database will be stored.
db = QuixDB.connect(DB_FOLDER) # Connect to the database named "UserData". witch is a fodler in the current directory.  

# create a table if it does not exist. and if it exists do nothing
db.create_table("Users", columns={"Email": str, "Name": str, "Password": str}, unique=["Email"]) # Create a table named "Users" with specified columns. make email unique.

def add_user(email, name, password):
    # Insert a new user into the "Users" table.
    if db.insert("Users", data={"Email": email, "Name": name, "Password": password}):
        return True
    return False

def login_user(email, password):
    # Retrieve a user from the "Users" table based on email and password.
    user = db.select("Users", where={"Email": email, "Password": password})
    if user:
        return user  # Return the first matching user.
    return False

def update_user(email, password, new_email, new_password):
    # Update a user's email and password in the "Users" table based on their name.
    if db.update("Users", where={"Email": email, "Password": password}, data={"Email": new_email, "Password": new_password}):
        return True
    return False

def delete_user(email, password):
    # Delete a user from the "Users" table based on email and password.
    if db.delete("Users", where={"Email": email, "Password": password}):
        return True
    return False
    
# Example usage: add user to the database
if add_user("Steve@apple.com", "Steve Jobs", "apple123"):
    print("User added successfully.")
else:
    print("Failed to add user.")

# Example usage: login user from the database, and return only Email and password from the user. not the name.
d = login_user("Steve@apple.com", "apple123")
if d:
    print("Login successful. user data: Email and password", d)
else:
    print("Login failed.")

# Example usage: update user in the database
if update_user("Steve@apple.com", "apple123", "NewSteve@apple.com", "newpassword123"):
    print("User updated successfully.")
else:
    print("Failed to update user.")

# Example usage: delete user from the database
if delete_user("NewSteve@apple.com", "newpassword123"):
    print("User deleted successfully.")
else:
    print("Failed to delete user.")

```
# Ideal Use Cases
 - Small-scale projects with low write frequency
 - Educational purposes for learning database concepts
 - Prototyping before moving to full database systems
 - Embedded systems with limited resources

# Configuration storage requiring structured data

# Limitations
 - Not suitable for high-write environments
 - No SQL query parser (basic filtering only)
 - No transaction rollback capability
 - No built-in data encryption

# Limited to single-node operation

# Performance Considerations
 - Reads are O(n) without unique constraints
 - Unique constraint lookups are O(1)
 - Write operations require full table locking
 - Large datasets may experience performance degradation

File Locking Mechanism
 - QuixDB uses atomic file locking to prevent race conditions:
 - Lock files are created during write operations
 - Processes retry lock acquisition every 10ms
 - Locks are automatically released after operations
 - PID-based locking prevents cross-process conflicts
