{'hashtags.0': {$exists: true}}



db.tweet.aggregate([
    {"$group" : {_id:"$user_id",
                    totalReceivedRetweetCount: {$sum: "$nbr_retweet" },
                    totalReceivedReplyCount: {$sum: "$nbr_reply" },
                    totalTweetCount:{$sum:1}, totalRetweetCount: {$sum: {$cond: [ "$is_retweet", 1, 0] }},
                    totalReplyCount: {$sum: {$cond: [ "$is_reply", 1, 0] }}}},
    {$project:   {user_id:1,
                   totalReceivedRetweetCount:1,
                   totalReceivedReplyCount:1,
                   totalTweetCount:1,
                   totalRetweetCount : 1,
                   totalReplyCount:1,
                   factor:{$add: ["$totalReceivedRetweetCount", "$totalReceivedReplyCount"]}}},
    {$sort: ({factor:-1})},{$limit: 30}], { allowDiskUse: true }
    ).pretty()