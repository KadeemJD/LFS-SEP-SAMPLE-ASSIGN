# ------------------------------------------------
# R Script to Assign Samples from SAV to Postgres
# ------------------------------------------------

# Load Libraries
library(haven)        # For reading SAV
library(DBI)          # Database interface
library(RPostgres)    # PostgreSQL driver
library(rpostgis)     # Postgis driver
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
  ) %>%
  filter(!ed_2022 %in% c("19-072-00", "19-070-00", "29-075-10"))  # Exclude Mennonite communities

cat("Unique records in SAV file: ", n_distinct(sav_data$concat_key), "\n")

# ------------------------------
# Connect to PostgreSQL
# ------------------------------
cat("Connecting to PostgreSQL...\n")
conn <- tryCatch({
  dbConnect(
    RPostgres::Postgres(),
    host = db_host,
    port = db_port,
    dbname = db_name,
    user = db_user,
    password = db_password
  )
},error = function(e) {
  cat("❌ Connection error: ", conditionMessage(e), "\n")
  return(NULL)
})

# ------------------------------
# Reset sampled column
# ------------------------------
cat("Resetting sampled flags to NULL...\n")
reset_query <- glue("UPDATE {db_table} SET sampled = NULL;")
dbExecute(conn, reset_query)

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
# Match & Filter Surveys Database
# ------------------------------
cat("Matching records...\n")
surveys_matched <- sav_data %>%
  filter(concat_key %in% db_data$concat_key)

surveys_unmatched <- sav_data %>%
  filter(!concat_key %in% db_data$concat_key)

cat("surveys_matched records: ", nrow(surveys_matched), "\n")
cat("surveys_unmatched records: ", nrow(surveys_unmatched), "\n")

# Only proceed if all SAV records matched
if (nrow(surveys_unmatched) == 0) {
  
  # Save matches
  write_csv(surveys_matched, "surveys_matched_records.csv")
  
  # ------------------------------
  # Update Sampled Flag in DB
  # ------------------------------
  cat("Updating matched records...\n")
  
  update_query <- glue("\n  UPDATE {db_table}\n  SET sampled='1'\n  WHERE interview__key=$1 AND ed_2022=$2;\n  ")
  
  updated_rows <- list()  # For tracking successful updates
  
  dbBegin(conn)
  for (i in 1:nrow(surveys_matched)) {
    row <- surveys_matched[i, ]
    tryCatch({
      dbExecute(conn, update_query, params = list(row$interview__key, row$ed_2022))
      updated_rows[[length(updated_rows)+1]] <- row
    }, error = function(e) {
      cat(glue("❌ Error updating row {i}: {e$message}\n"))
    })
  }
  dbCommit(conn)
  
  if (length(updated_rows) > 0) {
    updated_df <- dplyr::bind_rows(updated_rows)
    readr::write_csv(updated_df, "updated_records.csv")
    cat(glue("✅ Total updated records: {nrow(updated_df)}\n"))
  } else {
    cat("⚠️ No records were successfully updated.\n")
  }
  
} else {
  cat("⚠️ Script aborted. There are unmatched SAV records. No updates made.\n")
  write_csv(surveys_unmatched, "surveys_unmatched_records.csv")
}

# ------------------------------
# Clean up
# ------------------------------
dbDisconnect(conn)
cat("Disconnected.\n")




