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

#--------------------------------------------------------
# Code to check in the .env file is being read from
#--------------------------------------------------------
# Sys.getenv("DB_NAME")
# Sys.getenv("DB_HOST")
# Sys.getenv("DB_USER")
# Sys.getenv("DB_PASSWORD")
# 
# cat("Connecting with:\n")
# cat("Host: ", db_host, "\n")
# cat("Port: ", db_port, "\n")
# cat("DB: ", db_name, "\n")
# cat("User: ", db_user, "\n")



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
  
#---------------------------------------------------------------- 
#Code just to check if there is a connection to the database
#----------------------------------------------------------------

# if (DBI::dbIsValid(conn)) {
#   cat("✅ Connected to the database.\n")
# } else {
#   cat("❌ Failed to connect to the database.\n")
# }


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

# Optional: Save surveys_unmatched to CSV
 write_csv(surveys_unmatched, "surveys_unmatched_records.csv")
 
 #save surveys_matched to csv
 write_csv(surveys_matched, "surveys_matched_records.csv")
 
 
 #--------------------------------------------------------------------
 #Match surveys_unmatched Against Census Archives
 #---------------------------------------------------------------------
 
 cat("Checking surveys_unmatched records against post_census_2022_building...\n")
 
 # Query post_census_2022_building
 census_query <- "SELECT interview__key, ed_2022, blk_newn_2022, bldg_newn FROM post_census_2022_building;"
 census_data <- dbGetQuery(conn,census_query) %>%
   mutate(
     concat_key=paste(interview__key, ed_2022, blk_newn_2022,bldg_newn, sep="-")
   )
 
 # Match with surveys_unmatched from surveys against post_census
 post_census_matched<- surveys_unmatched %>%
   filter(concat_key %in% census_data$concat_key)
 
 still_unmatched<-surveys_unmatched %>%
   filter
 
cat("post_census_2022_building matches: ", nrow(post_census_matched), "\n")
cat("Still unmatched after post census check: ", nrow(still_unmatched), "\n")

#save second level matches to CSV
write_csv(post_census_matched, "post_census_matched_records.csv")

#save still unmatched
write_csv(still_unmatched,"still_unmatched.csv")


#----------------------------------------------------------
# Match with surveys_unmatched from surveys against mics7 
#----------------------------------------------------------


# Query mics7 building
mics7_query <- "SELECT interview__key, ed_2022, blk_newn_2023, bldg_newn FROM mics7_building;"
mics7_data <- dbGetQuery(conn,mics7_query) %>%
  mutate(
    concat_key=paste(interview__key, ed_2022, blk_newn_2023,bldg_newn, sep="-")
  )

mics7_matched<- surveys_unmatched %>%
  filter(concat_key %in% mics7_data$concat_key)

still_unmatched_mics7<-surveys_unmatched %>%
  filter

cat("mics7_matched records: ", nrow(mics7_matched), "\n")
cat("still_unmatched_mics7 records: ", nrow(still_unmatched_mics7), "\n")

# Save still_unmatched_mics7 to CSV
write_csv(still_unmatched, "still_unmatched_mics7_records.csv")

#save mics7_matched to csv
write_csv(mics7_matched, "mics7_matched_records.csv")

#---------------------------------------------------------
# Match with surveys_unmatched from surveys against pes 
#---------------------------------------------------------

# Query pes building
pes_query <- "SELECT interview__key, ed_2020, blk_newn_2023, bldg_newn FROM pes_building_2020;"
pes_data <- dbGetQuery(conn,pes_query) %>%
  mutate(
    concat_key=paste(interview__key, ed_2020, blk_newn_2023,bldg_newn, sep="-")
  )

pes_matched<- surveys_unmatched %>%
  filter(concat_key %in% pes_data$concat_key)

still_unmatched_pes<-surveys_unmatched %>%
  filter

cat("pes_matched records: ", nrow(pes_matched), "\n")
cat("still_unmatched_pes records: ", nrow(still_unmatched_pes), "\n")

# Save still_unmatched_pes to CSV
write_csv(still_unmatched_pes, "still_unmatched_pes_records.csv")

#save surveys_matched to csv
write_csv(pes_matched, "pes_matched_records.csv")



# ------------------------------
# Update Sampled Flag in DB
# ------------------------------
cat("Updating matched records...\n")

update_query <- glue("
UPDATE {db_table}
SET sampled='1'
WHERE interview__key=$1 AND ed_2022=$2;
")




updated_rows <- list()  # For tracking successful updates

dbBegin(conn)

for (i in 1:nrow(matched)) {
  row <- matched[i, ]
  tryCatch({
    dbExecute(conn, update_query, params = list(row$interview__key, row$ed_2022))
    
    # ✅ Add row to tracking list
    updated_rows[[length(updated_rows)+1]]<-row
    
  }, error = function(e) {
    cat(glue("❌ Error updating row {i}: {e$message}\n"))
  })
}

dbCommit(conn)

# ✅ Convert list to data frame and save as CSV
if (length(updated_rows) > 0) {
  updated_df <- dplyr::bind_rows(updated_rows)
  readr::write_csv(updated_df, "updated_records.csv")
  cat(glue("✅ Total updated records: {nrow(updated_df)}\ n"))
} else {
  cat("⚠️ No records were successfully updated.\n")
}



cat("Update complete!\n")

# ------------------------------
# Clean up
# ------------------------------
dbDisconnect(conn)
cat("Disconnected.\n")




