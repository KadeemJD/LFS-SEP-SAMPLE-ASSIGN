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
#Uncomment this when you need to find the unmatched records from surveys database
# census_db_name <- Sys.getenv("CENSUS_DB_NAME")
# census_table <- Sys.getenv("CENSUS_TABLE")

# ------------------------------
# Read SAV File
# ------------------------------

cat("Reading SAV file...\n")
sav_data <- read_sav("SEPT2025SAMPLE.sav")

# Rename column for consistency
sav_data <- sav_data %>%
  rename(ed_2022 = ed,
         blk_newn_2022=block,
         bldg_newn=building_number) %>%
  mutate(
    concat_key = paste(interview__key, ed_2022, blk_newn_2022, bldg_newn, sep = "-")
  ) %>%
  filter(!ed_2022 %in% c("19-070-00"))  # Exclude Mennonite communities

sav_data <- sav_data %>% distinct(concat_key, .keep_all=TRUE)

cat("Unique records in SAV file: ", n_distinct(sav_data$concat_key), "\n")

# ------------------------------
# Connect to PostgreSQL databases 
# ------------------------------

#Connecting to surveys (aka test2) database

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

# #Connecting to census_archives database
# Un- comment this when we need to make a connection to census archives database and search for unmatched records
#
# cat("Connecting to census_archives DB...\n")
# conn_census <- tryCatch({
#   dbConnect(
#     RPostgres::Postgres(),
#     host = db_host,
#     port = db_port,
#     dbname = census_db_name,
#     user = db_user,
#     password = db_password
#   )
# },error = function(e) {
#   cat("❌ Connection error: ", conditionMessage(e), "\n")
#   return(NULL)
# })


# # ------------------------------
# # Reset sampled column
# # ------------------------------

# Uncomment when you need to clear all the records from the column "sampled"

# cat("Resetting sampled flags to NULL...\n")
# reset_query <- glue("UPDATE {db_table} SET sampled = NULL;")
# dbExecute(conn_surveys, reset_query)

# ------------------------------
# Read from Database (aka test2)
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
#This is also used when we have determined which records from surveys and which records from post_census to use
#so you can do the last match and filter

cat("Matching records...\n")
surveys_matched <- sav_data %>%
  filter(concat_key %in% db_data$concat_key)

surveys_unmatched <- sav_data %>%
  filter(!concat_key %in% db_data$concat_key)

cat("surveys_matched records: ", nrow(surveys_matched), "\n")
cat("surveys_unmatched records: ", nrow(surveys_unmatched), "\n")

write_csv(surveys_unmatched, "surveys_unmatched_records.csv")
write_csv(surveys_matched, "surveys_matched_records.csv")

# # ------------------------------------------------------------------
# # Match surveys_unmatched against post_census_2022_building
# # ------------------------------------------------------------------
# Uncomment this if you need to checked unmatched from surveys against post census
# 
# cat("Checking post_census_2022_building...\n")
# census_query <- "SELECT interview__key, ed_2022, blk_newn_2022, bldg_newn FROM post_census_2022_building;"
# census_data <- dbGetQuery(conn_census, census_query) %>%
#   mutate(concat_key = paste(interview__key, ed_2022, blk_newn_2022, bldg_newn, sep = "-"))
# 
# post_census_matched <- census_data %>% filter(concat_key %in% surveys_unmatched$concat_key)
# surveys_unmatched <- surveys_unmatched %>% filter(!concat_key %in% census_data$concat_key)
# 
# cat("post_census_matched records: ", nrow(post_census_matched), "\n")
# cat("still unmatched after census check: ", nrow(surveys_unmatched), "\n")
# 
# write_csv(post_census_matched, "post_census_matched_records.csv")
# 

#Possible deletion
# # Making sure that fields are numeric and getting rid of leading zeros
# surveys_matched <- surveys_matched %>%
#   mutate(blk_newn_2022 = as.character(blk_newn_2022),
#          bldg_newn = as.character(bldg_newn))
# 
# post_census_matched <- post_census_matched %>%
#   mutate(blk_newn_2022 = as.character(blk_newn_2022),
#          bldg_newn = as.character(bldg_newn))


#Possible deletion
# # Combine all matched
# all_matched <- bind_rows(surveys_matched, post_census_matched) #mics7_matched, pes_matched)
# 
# # Save final unmatched to CSV
# write_csv(surveys_unmatched, "final_unmatched_records.csv")

# Only proceed if all SAV records matched
if (nrow(surveys_unmatched) == 0) {
  
  write_csv(all_matched, "all_matched_records.csv")
  
  # --------------------------------------------------------
  # Update Sampled Flag in DB Initial from Surveys database
  # --------------------------------------------------------
  
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
# Update from post_census_matched → back to test2 using 3-key match
# ------------------------------

cat("Linking post_census_matched back to test2 for remaining updates...\n")

# Reload test2.sde.lfs_general_building with all relevant columns
test2_query <- glue("SELECT interview__key, ed_2022, blk_newn_2022, bldg_newn FROM {db_table};")
test2_data <- dbGetQuery(conn_surveys, test2_query)

# Ensure all key fields are in same type (character preferred for joining)
test2_data <- test2_data %>%
  mutate(
    ed_2022 = as.character(ed_2022),
    blk_newn_2022 = as.character(blk_newn_2022),
    bldg_newn = as.character(bldg_newn),
    three_key = paste(ed_2022, blk_newn_2022, bldg_newn, sep = "-")
  )

post_census_matched <- post_census_matched %>%
  mutate(
    ed_2022 = as.character(ed_2022),
    blk_newn_2022 = as.character(blk_newn_2022),
    bldg_newn = as.character(bldg_newn),
    three_key = paste(ed_2022, blk_newn_2022, bldg_newn, sep = "-")
  )

# Join back using 3-key and select correct version of columns
back_matched <- inner_join(
  post_census_matched,
  test2_data,
  by = "three_key"
) %>% transmute(
  interview__key = interview__key.y,  # from test2_data
  ed_2022 = ed_2022.y
)

cat("Records matched back to test2 using 3-key match: ", nrow(back_matched), "\n")

# ---------------------------------
# Update Sampled Flag in test2 DB
# ---------------------------------

cat("🔁 Updating test2.sde.lfs_general_building for these remaining matches...\n")

update_query_3key <- glue("
  UPDATE {db_table}
  SET sampled = '1'
  WHERE interview__key = $1 AND ed_2022 = $2;
")

updated_rows_3key <- list()
dbBegin(conn_surveys)
for (i in 1:nrow(back_matched)) {
  row <- back_matched[i, ]
  tryCatch({
    dbExecute(conn_surveys, update_query_3key, params = list(row$interview__key, row$ed_2022))
    updated_rows_3key[[length(updated_rows_3key) + 1]] <- row
  }, error = function(e) {
    cat(glue("❌ Error updating row {i}: {e$message}\n"))
  })
}
dbCommit(conn_surveys)

# Save CSV
if (length(updated_rows_3key) > 0) {
  updated_df_3key <- bind_rows(updated_rows_3key)
  write_csv(updated_df_3key, "updated_from_census_backmatch.csv")
  cat(glue("✅ Additional updates applied using 3-key backmatch: {nrow(updated_df_3key)}\n"))
} else {
  cat("⚠️ No additional records were updated using 3-key backmatch.\n")
}


# Identify unmatched from back-match
back_unmatched <- post_census_matched %>%
  filter(!three_key %in% back_matched$three_key)

cat("❌ Records found in post_census_2022_building but NOT found in test2 using 3-key: ", nrow(back_unmatched), "\n")

write_csv(back_unmatched, "back_unmatched_not_in_test2.csv")




# ------------------------------
# Final verification
# ------------------------------

cat("✅ Verifying final sample count in test2...\n")
final_check <- dbGetQuery(conn_surveys, glue("SELECT COUNT(*) FROM {db_table} WHERE sampled = '1';"))
cat(glue("✅ Final count of sampled buildings in test2: {final_check[[1]]} (should be 2967)\n"))


# ------------------------------
# Clean up
# ------------------------------

dbDisconnect(conn_surveys)
dbDisconnect(conn_census)
cat("Disconnected from both databases.\n")




