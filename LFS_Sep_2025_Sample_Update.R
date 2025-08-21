# ------------------------------------------------------------------
# ⫸⫸⫸⫸⫸R Script to Assign Samples from SAV to Postgres⫷⫷⫷⫷⫷#
# ------------------------------------------------------------------


#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#01 Load Libraries
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

library(haven)        # For reading SAV
library(DBI)          # Database interface
library(RPostgres)    # PostgreSQL driver
library(rpostgis)     # Postgis driver
library(dplyr)        # Data manipulation
library(glue)         # For string interpolation
library(readr)        # Optional for CSV export
library(dotenv)       # For loading .env config file

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#02 Load Config from .env File
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

load_dot_env(file = "config.env")

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#03-1 Get environment variables from main database (test2 aka surveys)
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

db_host <- Sys.getenv("DB_HOST")
db_port <- as.integer(Sys.getenv("DB_PORT"))
db_name <- Sys.getenv("DB_NAME")
db_user <- Sys.getenv("DB_USER")
db_password <- Sys.getenv("DB_PASSWORD")
db_table <- Sys.getenv("DB_TABLE")
sav_path <- Sys.getenv("SAV_PATH")

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#03-2 Connection to census_archives database
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

#🔓~~Uncomment the code below when you need to find the unmatched records from surveys database as well as
#section 11 (Match surveys_unmatched against post_census_2022_building) below~~ 🔓#

# census_db_name <- Sys.getenv("CENSUS_DB_NAME")
# census_table <- Sys.getenv("CENSUS_TABLE")

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#04 Read SAV File
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

cat("Reading SAV file...\n")
sav_data <- read_sav("SEPT2025SAMPLE.sav")
#training_data <- read_sav("FINAL_LFS_09_2025_Training.sav)

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#05 Rename column for consistency
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

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

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#06-1 Connect to PostgreSQL databases 
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

#📁Connecting to surveys (aka test2) database📁#

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

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#06-2 Connecting to census_archives database
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

#🔓~~Uncomment the code below when we need to make a connection to census archives database and search for unmatched records~~🔓#
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


#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#07 Reset sampled column
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

#🔓~~Uncomment the code below when you need to clear all the records from the column "sampled"~~🔓#

# cat("Resetting sampled flags to NULL...\n")
# reset_query <- glue("UPDATE {db_table} SET sampled = NULL;")
# dbExecute(conn_surveys, reset_query)

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#08 Read from Database (aka test2)
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

cat("Fetching DB data...\n")
query <- glue("SELECT interview__key, ed_2022, blk_newn_2022, bldg_newn FROM {db_table};")
db_data <- dbGetQuery(conn_surveys, query) %>%
  mutate(
    concat_key = paste(interview__key, ed_2022, blk_newn_2022, bldg_newn, sep = "-")
  )

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#09 Match & Filter Surveys Database
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

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



#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#10 Export unique EDs in a csv separately
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
# 
#🔓~~Uncomment the code below and run it independently when you need to get the matched unique eds from the matched records~~🔓# 
#
# # For surveys_matched
# unique_surveys_eds <- surveys_matched %>%
#   select(ed_2022) %>%
#   distinct()
# 
# write_csv(unique_surveys_eds, "unique_surveys_matched_eds.csv")
# cat(glue("✅ Exported {nrow(unique_surveys_eds)} unique EDs from surveys_matched to 'unique_surveys_matched_eds.csv'\n"))
# 
# # For post_census_matched, if it exists
# if (exists("post_census_matched")) {
#   unique_postcensus_eds <- post_census_matched %>%
#     select(ed_2022) %>%
#     distinct()
#   
#   write_csv(unique_postcensus_eds, "unique_post_census_matched_eds.csv")
#   cat(glue("✅ Exported {nrow(unique_postcensus_eds)} unique EDs from post_census_matched to 'unique_post_census_matched_eds.csv'\n"))
# }


#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#11 Match surveys_unmatched against post_census_2022_building
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

#🔓~~Uncomment the code below aswell as section "03-2" (Connection to census_archives database) above 
#if you need to check unmatched from surveys against post census~~🔓#
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

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#12 Typecasting column names in the databases
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#


surveys_matched <- surveys_matched %>% 
  mutate(surveys_matched, blk_newn_2022=as.double(blk_newn_2022)) %>% 
  mutate(surveys_matched, bldg_newn=as.double(bldg_newn))

post_census_matched <- post_census_matched %>% 
  mutate(post_census_matched, blk_newn_2022=as.double(blk_newn_2022)) %>% 
  mutate(post_census_matched, bldg_newn=as.double(bldg_newn))

#Combine all matched
all_matched <- bind_rows(surveys_matched, post_census_matched) 
                 


# Only proceed if all SAV records matched
if (nrow(surveys_unmatched) == 0) {
  
  write_csv(all_matched, "all_matched_records.csv")
  
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#13 Update Sampled Flag in DB Initial from Surveys database
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
  
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


#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#14 Final verification
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

cat("✅ Verifying final sample count in test2...\n")
final_check <- dbGetQuery(conn_surveys, glue("SELECT COUNT(*) FROM {db_table} WHERE sampled = '1';"))
message <- glue("✅ Final count of sampled buildings in test2: {final_check[[1]]} (should be 2967)\n")
cat(message, "\n")

#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#
#15 Clean up
#░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░#

dbDisconnect(conn_surveys)
dbDisconnect(conn_census)
cat("Disconnected from both databases.\n")




