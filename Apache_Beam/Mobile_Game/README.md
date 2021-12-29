# Mobile Game Streaming Pipeline with Apache Beam


## Description

We have 1 script called `publish.py` that reads in the text file line by line and publish it to the PubSub topic.
The second script called `processing.py` show the main logic, where first two branches of the pipeline compute the increment scores
of Players and Teams (1 kill = 1 score, as 1 record in the file is equivalent to 1 kill). The last branch is to compute
the average number of points per game, per player, per weapon to see where the player has more skill. 


### Required ddependencies

- `pip install google-cloud-pubsub`
- `pip install apache-beam`
- `pip install 'apache-beam[gcp]'`
- `pip install google-apitools`