library(haven)
library(tidyr)
library(dplyr)
library(stringdist)

#Importing dataset
df <- read_dta("C:/Users/shree/Dropbox/DAI_WB_TN/baseline/clean_data/Baseline_clean.dta")

View(df) #View original baseline data

#Keeping relevant variables (Name of respondent, application ID, district, block, gp, village, household roster)
selected_df_name <- df[, c("b5_name", "application_id", "districtname","blockname", "gp_id2","submissiondate", "village", "member_1", "member_2", "member_3", "member_4", "member_5", "member_6", "member_7","member_8")]

View(selected_df_name) #View relevant data

# Use pivot_longer to transform member columns into member_id and member_name
df_long <- selected_df_name %>%
  pivot_longer(
    cols = starts_with("member_"),
    names_to = "member_id",
    values_to = "member_name"
  )

# Filter data to drop observations with blank member names
df_filtered <- df_long %>% filter(member_name != "")

View(df_filtered) #View filtered long data

#Performing an inner join of each member of a household with each member of all the households within a district
df_join <- inner_join(df_filtered, df_filtered, by="districtname")           

View(df_join) #View joined data

################################################################################

#Stage I: Matching respondent name against the roster of all other households within the same district

# Filtering the joint data to keep only the observations where one household is matched against a different household a performing a string match of the respondent name (x) and the roster name (y) using Jarro Winkler method, keeping the threshold as 0.125
df_join_filtered <- df_join %>% filter(application_id.x!=application_id.y & stringdist(tolower(b5_name.x),tolower(member_name.y), method = "jw")<0.125)

#Keeping distinct pairs of matched application_id for readability
distinct_data <- df_join_filtered %>% distinct(application_id.x, application_id.y, .keep_all = TRUE)

View(distinct_data) #View distinct pairs

################################################################################

#Stage II: Matching the roster names of each household against roster names of each of the other households within the same district

#Matching each roster name for an ID to each roster name for a different ID within same district and filtering for jw<0.15
df_join_roster <- df_join %>% filter(application_id.x!=application_id.y & stringdist(tolower(member_name.x),tolower(member_name.y), method = "jw")<0.15)

View(df_join_roster) #View matched pairs

#For each match, group by unique pairs of IDs matched, generate a variable "count" that is the number of matches found in two matched IDs
result <- df_join_roster %>%
  group_by(application_id.x, application_id.y) %>%
  summarise(count = n())

#Filtering results where count is greater than or equal to two
result_filtered <- result %>% filter(count>=2)

View(result_filtered) #View filtered results

#Produces a list of all the matched pairs where number of matches in roster >=2
df_join_roster_filtered <- df_join_roster %>%
  semi_join(result_filtered, by = c("application_id.x", "application_id.y"))
View(df_join_roster_filtered)

#Keeping distinct pairs for readability
df_join_roster_distinct<- df_join_roster_filtered %>% distinct(application_id.x, application_id.y, .keep_all = TRUE)

View(df_join_roster_distinct) #View dataframe

#Exporting as csv
readr::write_csv(df_join_roster_distinct, "C:/Users/shree/Dropbox/DAI_WB_TN/baseline/clean_data/matched_rosters_pure_jw015.csv")

################################################################################

#Stage III: Using dataframe with more than 2 matches (Stage II) and dataframe where respondent name matches with name in other IDs roster

#Joining Stage I and Stage 2 data by each unique combination of matched IDs
result_final <- df_join_roster_filtered %>%
  semi_join(distinct_data, by = c("application_id.x", "application_id.y"))

#Keeping only distinct pairs
result_final_distinct<- result_final %>% distinct(application_id.x, application_id.y, .keep_all = TRUE)

View(result_final_distinct) #View result

#Exporting as csv
readr::write_csv(result_final_distinct, "C:/Users/shree/Dropbox/DAI_WB_TN/baseline/clean_data/matched_rosters_jw0125.csv")
