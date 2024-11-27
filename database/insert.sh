#!/bin/bash

# Start a transaction and insert data
echo "Starting data insertion process..."

# Open SQLite and set optimizations
sqlite3 database.sqlite <<EOF
PRAGMA foreign_keys = OFF;
PRAGMA synchronous = OFF;
PRAGMA journal_mode = MEMORY;
BEGIN TRANSACTION;
EOF

# Define batch size and initialize an empty string to hold values
batch_size=90  # You can adjust the batch size for optimal performance
values=""

# Loop through the user data
for i in {1..150000}
do
  # Append new row to the values string
  values+="('Name $i', 'emailjjkk$i@eslxamples.com', datetime('now'), 'hashedpassword', 'randomtoken', datetime('now'), datetime('now')),"

  # If the batch size is reached, insert the batch into SQLite
  if ((i % batch_size == 0)); then
    # Remove trailing comma
    values="${values%,}"

    # Insert the batch into SQLite using a single query
    sqlite3 database.sqlite "INSERT INTO users (name, email, email_verified_at, password, remember_token, created_at, updated_at) VALUES $values;"

    # Reset values for the next batch
    values=""
  fi
done

# If there are any remaining records to insert (in case the total is not perfectly divisible by batch size)
if [ -n "$values" ]; then
  values="${values%,}"
  sqlite3 database.sqlite "INSERT INTO users (name, email, email_verified_at, password, remember_token, created_at, updated_at) VALUES $values;"
fi

# Commit the transaction to save all the changes
sqlite3 database.sqlite <<EOF
COMMIT;
EOF

# Re-enable foreign key checks and reset PRAGMA settings (optional)
sqlite3 database.sqlite <<EOF
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;
EOF

echo "Inserted 150,000 records into the users table."
