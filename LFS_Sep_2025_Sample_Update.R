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

# Get environment variables from main database (test2 aka surveys)
db_host <- Sys.getenv("DB_HOST")
db_port <- as.integer(Sys.getenv("DB_PORT"))
db_name <- Sys.getenv("DB_NAME")
db_user <- Sys.getenv("DB_USER")
db_password <- Sys.getenv("DB_PASSWORD")
db_table <- Sys.getenv("DB_TABLE")
sav_path <- Sys.getenv("SAV_PATH")

# census_archives database

census_db_name <- Sys.getenv("CENSUS_DB_NAME")
census_table <- Sys.getenv("CENSUS_TABLE")

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
  filter(!ed_2022 %in% c("19-070-00"))  # Exclude Mennonite communities

sav_data <- sav_data %>% distinct(concat_key, .keep_all=TRUE)

cat("Unique records in SAV file: ", n_distinct(sav_data$concat_key), "\n")

# ------------------------------
# Connect to PostgreSQL databases
# ------------------------------

#Connecting to surveys database

cat("Connecting to Surveys DB...\n")
conn_surveys <- tryCatch({
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

#Connecting to census_archives database

cat("Connecting to census_archives DB...\n")
conn_census <- tryCatch({
  dbConnect(
    RPostgres::Postgres(),
    host = db_host,
    port = db_port,
    dbname = census_db_name,
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
db_data <- dbGetQuery(conn_surveys, query) %>%
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

# ------------------------------------------------------------------
# Match surveys_unmatched against post_census_2022_building
# ------------------------------------------------------------------
cat("Checking post_census_2022_building...\n")
census_query <- "SELECT interview__key, ed_2022, blk_newn_2022, bldg_newn FROM post_census_2022_building;"
census_data <- dbGetQuery(conn_census, census_query) %>%
  mutate(concat_key = paste(interview__key, ed_2022, blk_newn_2022, bldg_newn, sep = "-"))

post_census_matched <- surveys_unmatched %>% filter(concat_key %in% census_data$concat_key)
surveys_unmatched <- surveys_unmatched %>% filter(!concat_key %in% census_data$concat_key)

cat("post_census_matched records: ", nrow(post_census_matched), "\n")
cat("still unmatched after census check: ", nrow(surveys_unmatched), "\n")

# # ------------------------------------------------------------------
# # Match against mics7_building
# # ------------------------------------------------------------------
# cat("Checking mics7_building...\n")
# mics7_query <- "SELECT interview__key, ed_2022, blk_newn_2023, bldg_newn FROM mics7_building;"
# mics7_data <- dbGetQuery(conn, mics7_query) %>%
#   mutate(concat_key = paste(interview__key, ed_2022, blk_newn_2023, bldg_newn, sep = "-"))
# 
# mics7_matched <- surveys_unmatched %>% filter(concat_key %in% mics7_data$concat_key)
# surveys_unmatched <- surveys_unmatched %>% filter(!concat_key %in% mics7_data$concat_key)
# 
# cat("mics7_matched records: ", nrow(mics7_matched), "\n")
# cat("still unmatched after mics7 check: ", nrow(surveys_unmatched), "\n")

# # ------------------------------------------------------------------
# # Match against pes_building_2020
# # ------------------------------------------------------------------
# cat("Checking pes_building_2020...\n")
# pes_query <- "SELECT interview__key, ed_2020, blk_newn_2023, bldg_newn FROM pes_building_2020;"
# pes_data <- dbGetQuery(conn, pes_query) %>%
#   mutate(concat_key = paste(interview__key, ed_2020, blk_newn_2023, bldg_newn, sep = "-"))
# 
# pes_matched <- surveys_unmatched %>% filter(concat_key %in% pes_data$concat_key)
# surveys_unmatched <- surveys_unmatched %>% filter(!concat_key %in% pes_data$concat_key)
# 
# cat("pes_matched records: ", nrow(pes_matched), "\n")
# cat("still unmatched after pes check: ", nrow(surveys_unmatched), "\n")

# Combine all matched
all_matched <- bind_rows(surveys_matched, post_census_matched) #mics7_matched, pes_matched)

# Save final unmatched to CSV
write_csv(surveys_unmatched, "final_unmatched_records.csv")

# Only proceed if all SAV records matched
if (nrow(surveys_unmatched) == 0) {
  
  write_csv(all_matched, "all_matched_records.csv")
  
  # ------------------------------
  # Update Sampled Flag in DB
  # ------------------------------
  cat("Updating matched records...\n")
  
  update_query <- glue("\n  UPDATE {db_table}\n  SET sampled='1'\n  WHERE interview__key=$1 AND ed_2022=$2;\n  ")
  
  updated_rows <- list()
  dbBegin(conn_surveys)
  for (i in 1:nrow(all_matched)) {
    row <- all_matched[i, ]
    tryCatch({
      dbExecute(conn_surveys, update_query, params = list(row$interview__key, row$ed_2022))
      updated_rows[[length(updated_rows)+1]] <- row
    }, error = function(e) {
      cat(glue("❌ Error updating row {i}: {e$message}\n"))
    })
  }
  dbCommit(conn_surveys)
  
  if (length(updated_rows) > 0) {
    updated_df <- dplyr::bind_rows(updated_rows)
    readr::write_csv(updated_df, "updated_records.csv")
    cat(glue("✅ Total updated records: {nrow(updated_df)}\n"))
  } else {
    cat("⚠️ No records were successfully updated.\n")
  }
  
} else {
  cat("⚠️ Script aborted. There are unmatched SAV records after checking all tables. No updates made.\n")
}

# ------------------------------
# Clean up
# ------------------------------
dbDisconnect(conn_surveys)
dbDisconnect(conn_census)
cat("Disconnected from both databases.\n")




