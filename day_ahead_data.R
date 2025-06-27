#This is an outdated method of collecting day-ahead auction data from EPEX SPOT.

setLogging(TRUE)
library(emarketcrawlR)
auction <- getDayAheadAuctionEPEXSPOT("2024-01-01", "2024-01-01", "DE")
# Save the data to a CSV file
write.csv(auction, "C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data and Collection/day_ahead_auction_data.csv", row.names = FALSE)