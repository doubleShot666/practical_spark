Find the favourite genre of a given user, or group of users：
	This is done by calculate the formula : count * factor + average_rating * ( 1 - factor) on users' ratings and movies watched.
	count is the number of specific genre movies watched.
 	average_rating is average rating of  user group to this genre's movie.
	factor is the weight value of count. That's to say, how much important do you think count is when deciding your favorite genre.

Compare the movie tastes of two users:
     	Movie taste is compared in two aspects. 
	1. Number of common watched movies by two users. Also the whole information of the movies and ratings of them will be showed in table.
                2. similarity score out of 10 based on two users' ratings on these common movies.
                First calculate point of each rating pairs and then sum them up. 
                Finally the sum would be averaged to make the similarity score.

	Calculation formular of point is as following:
	Same Rating (eg 5 and 5): 10pts,

	½ Rating difference (eg 4 and 4½): 8pts

	1 Rating difference (eg 5 and 4): 6pts

	1 ½ Rating difference (eg 4 and 2 ½): 5pts

	2 Rating difference (eg 4 and 2): 4pts

	2 ½ Rating difference (eg 4 and 1 ½): 3pts

	3 Rating difference (eg 4 and 1): 2pts

	3 ½ Rating difference (eg 5 and 1 ½): 1pt

	4 Rating difference (eg 5 and 1): 0.5pts

	4 ½ Rating difference (eg 5 and ½): 0 pts
                
	You can see that gap is not evenly distributed. that is because some thoughts on the significance of scoring gap.
	if User A were to rate a movie with 4.5 stars (meaning they thought it was near masterpiece level), and User B were to rate it with 3.5 stars
	 (meaning they thought it was good but still flawed), even though it’s only 1 star, 
	it’s still a noticeable gap in tastes that went from almost perfect to an passable film.
	Therefore, the similarity score wouldn’t really merit an 8 (which would mean someone you have many similarities with), 
	it would be more of a 6 (which means someone who kind of likes what you like but not to the same degree.
         	So, that’s why  it's best to have a larger gap between ratings, as the smaller the gap, the less the change in rating would mean.
	
	
