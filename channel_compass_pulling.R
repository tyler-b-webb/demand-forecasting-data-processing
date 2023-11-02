# loading required packages 
library(aws.s3)
library(jsonlite)
library(dplyr)

aws_bucket_name_b <- 'rsconnect-mars-np' 
bucket_b_creds <- jsonlite::fromJSON(system('aws --region us-east-1 secretsmanager get-secret-value --secret-id rsconnect-mars-np-access --query SecretString --output text', intern=TRUE)) 

aws_key_b <- bucket_b_creds$AWS_KEY 
aws_secret_b <- bucket_b_creds$AWS_SECRET

# Sales for corn
aws.s3::s3load('data_mars3/backend/cust_df_corn_my24.rda', bucket = aws_bucket_name_b, key = aws_key_b, secret = aws_secret_b, region = 'us-east-1')

# Sales for soy
aws.s3::s3load('data_mars3/backend/cust_df_soy_my24.rda', bucket = aws_bucket_name_b, key = aws_key_b, secret = aws_secret_b, region = 'us-east-1')

cust_df_corn_my24_subset <- cust_df_corn_my24[cust_df_corn_my24$Year == 2024, ]
cust_df_soy_my24_subset <- cust_df_soy_my24[cust_df_soy_my24$MKT_YR == 2024, ]

write.csv(cust_df_corn_my24_subset, '../NA-soy-pricing/data/channel/channel_24only_corn_10_5.csv')
write.csv(cust_df_soy_my24_subset, '../NA-soy-pricing/data/channel/channel_24only_soybean_10_5.csv')