# ------------------------------
# R Script to Assign Samples from SAV to Postgres
# ------------------------------

# Load Libraries
library(haven)        # For reading SAV
library(DBI)          # Database interface
library(RPostgres)    # PostgreSQL driver
library(rpostgis)
library(dplyr)        # Data manipulation
library(glue)         # For string interpolation
library(readr)        # Optional for CSV export
library(dotenv)       # For loading .env config file

# ------------------------------
# Load Config from .env File
# ------------------------------
load_dot_env(file = "config.env")

# Get environment variables
db_host <- Sys.getenv("DB_HOST")
db_port <- as.integer(Sys.getenv("DB_PORT"))
db_name <- Sys.getenv("DB_NAME")
db_user <- Sys.getenv("DB_USER")
db_password <- Sys.getenv("DB_PASSWORD")
db_table <- Sys.getenv("DB_TABLE")
sav_path <- Sys.getenv("SAV_PATH")

# ------------------------------
# Read SAV File
# ------------------------------
cat("Reading SAV file...\n")
sav_data <- read_sav("SEPT2025SAMPLE.sav")

# Rename column for consistency
sav_data <- sav_data %>%
  rename(ed_2022 = ed) %>%
  mutate(
    concat_key = paste(interview__key, ed_2022, block, building_number, sep = "-")
  )

cat("Unique records in SAV file: ", n_distinct(sav_data$concat_key), "\n")

# ------------------------------
# Connect to PostgreSQL
# ------------------------------
cat("Connecting to PostgreSQL...\n")
conn <- dbConnect(
  RPostgres::Postgres(),
  host = db_host,
  port = db_port,
  dbname = db_name,
  user = db_user,
  password = db_password
)

# ------------------------------
# Read from Database
# ------------------------------
cat("Fetching DB data...\n")
query <- glue("SELECT interview__key, ed_2022, blk_newn_2022, bldg_newn FROM {db_table};")
db_data <- dbGetQuery(conn, query) %>%
  mutate(
    concat_key = paste(interview__key, ed_2022, blk_newn_2022, bldg_newn, sep = "-")
  )

# ------------------------------
# Match & Filter
# ------------------------------
cat("Matching records...\n")
matched <- sav_data %>%
  filter(concat_key %in% db_data$concat_key)

unmatched <- sav_data %>%
  filter(!concat_key %in% db_data$concat_key)

cat("Matched records: ", nrow(matched), "\n")
cat("Unmatched records: ", nrow(unmatched), "\n")

# Optional: Save unmatched to CSV
# write_csv(unmatched, "unmatched_records.csv")

# ------------------------------
# Update Sampled Flag in DB
# ------------------------------
cat("Updating matched records...\n")

update_query <- glue("
UPDATE {db_table}
SET sampled='1'
WHERE interview_key=$1 AND ed_2022=$2;
")

# update_query <- glue(
#                      UPDATE {db_table}
#                      SET sampled = '1'
#                      WHERE interview__key = $1 AND ed_2022 = $2;
#                      ")

dbBegin(conn)

for (i in 1:nrow(matched)) {
  row <- matched[i, ]
  tryCatch({
    dbExecute(conn, update_query, params = list(row$interview__key, row$ed_2022))
  }, error = function(e) {
    cat("Error updating row ", i, ": ", e$message, "\n")
  })
}

dbCommit(conn)

cat("Update complete!\n")

# ------------------------------
# Clean up
# ------------------------------
dbDisconnect(conn)
cat("Disconnected.\n")




